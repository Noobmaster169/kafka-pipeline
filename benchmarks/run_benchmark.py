"""
AWAS performance harness — generates REAL evidence, never static numbers.

One benchmark run = drive the simulator at a chosen load against the live stack, then read
the resulting `violations` documents straight out of MongoDB to measure:

  * produce_eps       — the SIMULATOR's produce rate (events/s over the produce window).
                          NOT a pipeline metric: it saturates at the Python producer's speed
                          (~1300 ev/s) regardless of partitions. Reported for context only.
  * pipeline_eps      — the PIPELINE's end-to-end processing rate: events ÷ drain time,
                          where drain = (last violation's `detected_at`) − (run start).
                          THIS is the partition-scaling metric.
  * end-to-end latency — per violation, `detected_at - timestamp_end` (both already stored
                          in the document by the pipeline sink, so no extra instrumentation)

Before the measured run, a WARMUP batch (20 trips) is produced and the harness waits until a
violation lands in Mongo — proving the pipeline is actually consuming. Without this, a run
launched before Spark's streaming query is listening loses every event (the source reads
`startingOffsets=latest`) and records a useless 0-violation row. Skip with --no-warmup.

Each run is tagged with the knobs in effect (`KAFKA_TOPIC_PARTITIONS`, `SPARK_SHUFFLE_PARTITIONS`
and `JOIN_STRATEGY`) and appended to a CSV, so repeating it across knob settings builds
the sweep the report's §8 describes. Because changing the partition count needs a *fresh* topic
(Kafka cannot repartition in place — see REPORT.md §6), the partition sweep is run one setting
per stack, e.g.:

    KAFKA_TOPIC_PARTITIONS=1 stack.sh down && stack.sh up && stack.sh topics
    stack.sh pipeline                       # in another terminal
    python -m benchmarks.run_benchmark --label p1 --total 400 --fast
    # ... repeat with KAFKA_TOPIC_PARTITIONS=2, 4, 6 ...

The join-strategy sweep (§8 experiment 4) restarts only the PIPELINE, not the stack —
`JOIN_STRATEGY=all-pairs stack.sh pipeline` vs `JOIN_STRATEGY=adjacent stack.sh pipeline`
(`--reset` between runs), with the same env var set for this harness so the row is tagged.
The offline companion `benchmarks/join_compare.py` measures the strategies' join cost
directly (output rows + time vs cameras-per-lane k), no broker required.

Run it INSIDE the stack (so `kafka`/`mongo` resolve) — see benchmarks/README.md.

Caveat: latency is most meaningful in LIVE mode (`--rate`), where event times track wall clock.
In `--fast` mode event times are synthetic, so latency is reported but flagged unreliable; use
fast mode for throughput, live mode for latency.
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Make `common` and the simulator importable when run as `python -m benchmarks.run_benchmark`
# from the project root, or directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import config  # noqa: E402
from common.mongo import get_db  # noqa: E402

CSV_HEADER = [
    "ran_at", "label", "mode", "trips", "kafka_partitions", "shuffle_partitions", "join_strategy",
    "produce_seconds", "events_estimate", "produce_eps", "drain_seconds", "pipeline_eps",
    "violations_new", "instant", "average",
    "latency_n", "latency_p50_s", "latency_p95_s", "latency_max_s", "latency_reliable",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AWAS performance benchmark (real measurements).")
    p.add_argument("--label", default="run", help="Tag for this run (e.g. 'p1', 'p6').")
    p.add_argument("--total", type=int, default=400, help="Number of trips to generate.")
    p.add_argument("--rate", type=float, default=3.0, help="Trips/sec in live mode (ignored with --fast).")
    p.add_argument("--fast", action="store_true", help="Blast mode (best for throughput; latency unreliable).")
    p.add_argument("--cameras-per-lane", type=int, default=3,
                   help="Used only to estimate events = trips * cameras_per_lane.")
    p.add_argument("--settle", type=float, default=20.0,
                   help="Seconds to wait after producing for the pipeline to drain before reading Mongo.")
    p.add_argument("--reset-violations", action="store_true",
                   help="Drop the violations collection before the run for a clean measurement.")
    p.add_argument("--no-warmup", action="store_true",
                   help="Skip the warmup batch that proves the pipeline is consuming before measuring.")
    p.add_argument("--warmup-timeout", type=float, default=120.0,
                   help="Seconds to wait for the warmup violation before aborting.")
    p.add_argument("--out", default=str(Path(__file__).resolve().parent / "results.csv"),
                   help="CSV file to append the result row to.")
    p.add_argument("--python", default=sys.executable, help="Python used to launch the simulator subprocess.")
    return p.parse_args()


def _aware_utc(dt: datetime) -> datetime:
    """Coerce a (possibly naive) datetime to timezone-aware UTC for safe subtraction."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def run_simulator(args: argparse.Namespace) -> float:
    """Launch the simulator as a subprocess; return the produce wall-time in seconds."""
    cmd = [args.python, "-m", "simulator.run", "--total", str(args.total)]
    if args.fast:
        cmd.append("--fast")
    else:
        cmd += ["--rate", str(args.rate)]
    print(f"[bench] producing: {' '.join(cmd)}  (cwd=src)")
    started = time.time()
    # Run from src/ so `python -m simulator.run` resolves, inheriting the stack's env.
    subprocess.run(cmd, cwd=str(Path(__file__).resolve().parents[1] / "src"), check=True)
    return time.time() - started


def ensure_pipeline_consuming(args: argparse.Namespace) -> None:
    """Prove the pipeline is consuming BEFORE the measured run.

    Spark's streaming query takes tens of seconds to start listening after `stack.sh pipeline`
    launches, and the source reads `startingOffsets=latest` — so events produced before that
    moment are silently skipped (in --fast mode the WHOLE run is gone in under a second,
    yielding a useless 0-violation row). Produce a small batch, wait for a violation to land
    in Mongo, then delete the warmup docs so they cannot pollute the measurement.
    """
    coll = get_db()[config.COLL_VIOLATIONS]
    started = datetime.now(timezone.utc)
    warm = argparse.Namespace(**{**vars(args), "total": 20, "fast": True})
    print("[bench] warmup: producing 20 trips to confirm the pipeline is consuming ...")
    run_simulator(warm)
    deadline = time.time() + args.warmup_timeout
    while time.time() < deadline:
        if coll.count_documents({"detected_at": {"$gte": started}}) > 0:
            coll.delete_many({"detected_at": {"$gte": started}})
            print("[bench] warmup OK — pipeline is consuming; starting the measured run")
            return
        time.sleep(2)
    sys.exit(
        f"[bench] FATAL: no violation appeared within {args.warmup_timeout:.0f}s of the warmup "
        "batch. Is `stack.sh pipeline` running, and has it logged 'streaming query started'? "
        "(Events produced before the query is listening are skipped: startingOffsets=latest.)"
    )


def measure(args: argparse.Namespace, run_start: datetime, events_estimate: int) -> dict:
    """Read violations detected since run_start and compute drain + latency stats."""
    coll = get_db()[config.COLL_VIOLATIONS]
    docs = list(coll.find({"detected_at": {"$gte": run_start}}))

    instant = sum(1 for d in docs if d.get("violation_type") == "INSTANTANEOUS")
    average = sum(1 for d in docs if d.get("violation_type") == "AVERAGE")

    # Pipeline-side throughput: how long the pipeline took to work through the run, measured
    # from run start to the LAST violation written. This (not the producer's rate) is the
    # number that can respond to the partition count.
    detected = [_aware_utc(d["detected_at"]) for d in docs if isinstance(d.get("detected_at"), datetime)]
    drain_seconds = (max(detected) - run_start).total_seconds() if detected else float("nan")
    pipeline_eps = events_estimate / drain_seconds if detected and drain_seconds > 0 else float("nan")

    latencies: list[float] = []
    for d in docs:
        det, end = d.get("detected_at"), d.get("timestamp_end")
        if isinstance(det, datetime) and isinstance(end, datetime):
            secs = (_aware_utc(det) - _aware_utc(end)).total_seconds()
            # Keep only physically plausible end-to-end latencies.
            if 0.0 <= secs <= max(args.settle + 600.0, 660.0):
                latencies.append(secs)

    latencies.sort()

    def pct(p: float) -> float:
        if not latencies:
            return float("nan")
        idx = min(len(latencies) - 1, int(round(p * (len(latencies) - 1))))
        return latencies[idx]

    return {
        "drain_seconds": round(drain_seconds, 2) if detected else float("nan"),
        "pipeline_eps": round(pipeline_eps, 1) if detected else float("nan"),
        "violations_new": len(docs),
        "instant": instant,
        "average": average,
        "latency_n": len(latencies),
        "latency_p50_s": round(statistics.median(latencies), 3) if latencies else float("nan"),
        "latency_p95_s": round(pct(0.95), 3) if latencies else float("nan"),
        "latency_max_s": round(max(latencies), 3) if latencies else float("nan"),
        "latency_reliable": "no" if args.fast else "yes",
    }


def main() -> int:
    args = parse_args()
    coll = get_db()[config.COLL_VIOLATIONS]

    if args.reset_violations:
        deleted = coll.delete_many({}).deleted_count
        print(f"[bench] reset: cleared {deleted} existing violation docs")

    if not args.no_warmup:
        ensure_pipeline_consuming(args)

    run_start = datetime.now(timezone.utc)
    produce_seconds = run_simulator(args)

    print(f"[bench] produced in {produce_seconds:.1f}s; settling {args.settle:.0f}s for the pipeline to drain ...")
    time.sleep(args.settle)

    events_estimate = args.total * args.cameras_per_lane
    produce_eps = events_estimate / produce_seconds if produce_seconds > 0 else float("nan")
    stats = measure(args, run_start, events_estimate)

    row = {
        "ran_at": run_start.isoformat(),
        "label": args.label,
        "mode": "fast" if args.fast else f"live@{args.rate}/s",
        "trips": args.total,
        "kafka_partitions": config.KAFKA_TOPIC_PARTITIONS,
        "shuffle_partitions": config.SPARK_SHUFFLE_PARTITIONS,
        "join_strategy": config.JOIN_STRATEGY,   # tag with what the running pipeline was started with
        "produce_seconds": round(produce_seconds, 2),
        "events_estimate": events_estimate,
        "produce_eps": round(produce_eps, 1),
        **stats,
    }

    out = Path(args.out)
    # If an older results.csv has a different column set, rotate it aside rather than
    # appending misaligned rows to it.
    if out.exists():
        with out.open() as f:
            existing_header = f.readline().strip().split(",")
        if existing_header != CSV_HEADER:
            backup = out.with_name(out.name + ".old")
            out.rename(backup)
            print(f"[bench] {out.name} had an old column layout — moved it to {backup.name}")
    is_new = not out.exists()
    with out.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        if is_new:
            w.writeheader()
        w.writerow(row)

    print("\n=== benchmark result (appended to {}) ===".format(out))
    width = max(len(k) for k in CSV_HEADER)
    for k in CSV_HEADER:
        print(f"  {k:<{width}} : {row[k]}")
    print("\nNote: numbers above are measured from this run. Re-run across "
          "KAFKA_TOPIC_PARTITIONS / SPARK_SHUFFLE_PARTITIONS settings to build the sweep.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

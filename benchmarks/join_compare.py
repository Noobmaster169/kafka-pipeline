"""
Join-strategy comparison — the REPORT.md §5.2 / §8 experiment, measured not asserted.

Runs the SAME synthetic workload through `detect_average()` under both camera-pair
strategies and records, per cameras-per-lane value k:

  * join output rows   — the candidate-pair count the stream-join literature uses as the
                         unit of join cost (Kang et al.'s window-join cost model; ScaleJoin
                         reports throughput in comparisons/second). Expected: vehicles*(k-1)
                         for `adjacent` vs vehicles*k(k-1)/2 for `all-pairs`.
  * join wall time     — end-to-end time to materialise that output in local Spark.

The two strategies, in plain terms:

  * `adjacent`  — the A3 refinement: joins CONSECUTIVE cameras only (|Δindex| = 1),
                  emitting O(k) rows per vehicle. The production default.
  * `all-pairs` — the A2 baseline: joins EVERY distinct camera pair, emitting O(k²)
                  rows per vehicle. Retained purely as the measured comparison point.

Every vehicle is a "sneaky" driver at a constant 120 km/h between cameras (limit 90), so
EVERY candidate pair violates and the output-row count equals the candidate-pair count —
the measurement is exactly the O(k) vs O(k²) candidate space, with no filter noise.
Both strategies flag identical final violations (asserted by pipeline/verify_detect.py);
the gap between the two curves is pure surplus join work.

Runs OFFLINE in local Spark — no Kafka broker, no Mongo — same as verify_detect.
From the project root (or `stack.sh joincmp` inside the Spark container):

    python -m benchmarks.join_compare --cameras 3 5 10 15 --vehicles 200
    python -m benchmarks.join_compare --plot-only     # regenerate the PNG from the CSV (no Spark)

Outputs: join_compare.csv (rows appended) and join_compare.png (plot, needs matplotlib).
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

T0 = datetime(2024, 1, 1, 8, 0, 0)
CROSSING_GAP_S = 30        # one camera crossing every 30 s -> 1 km / 30 s = 120 km/h
SPEED_LIMIT = 90.0         # every segment average (120) violates -> output rows == candidate pairs

# Human-readable identity of each strategy — used in the printed table and the plot legend,
# so a reader never has to know the config-flag names to understand the result.
STRATEGY_LABEL = {
    "adjacent": "adjacent (A3 refinement: consecutive cameras only — O(k) rows/vehicle)",
    "all-pairs": "all-pairs (A2 baseline: every camera pair — O(k²) rows/vehicle)",
}

CSV_HEADER = [
    "ran_at", "cameras_k", "vehicles", "events", "strategy",
    "join_output_rows", "expected_rows", "join_seconds",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare adjacent vs all-pairs join strategies (offline).")
    p.add_argument("--cameras", type=int, nargs="+", default=[3, 5, 10, 15],
                   help="Cameras-per-lane values (k) to sweep.")
    p.add_argument("--vehicles", type=int, default=200, help="Vehicles per run; each crosses all k cameras.")
    p.add_argument("--out", default=str(Path(__file__).resolve().parent / "join_compare.csv"),
                   help="CSV file to append result rows to.")
    p.add_argument("--png", default=str(Path(__file__).resolve().parent / "join_compare.png"),
                   help="Plot output path.")
    p.add_argument("--plot-only", action="store_true",
                   help="Skip the Spark sweep; regenerate the plot from the existing CSV.")
    p.add_argument("--no-plot", action="store_true", help="Skip plot generation after the sweep.")
    return p.parse_args()


def expected_rows(strategy: str, k: int, vehicles: int) -> int:
    return vehicles * (k - 1) if strategy == "adjacent" else vehicles * k * (k - 1) // 2


def run_sweep(args: argparse.Namespace) -> list[dict]:
    """The Spark part: run both strategies per k on identical cached input."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
    )

    from pipeline.detect import CAMERA_PAIR_CLAUSE, detect_average

    schema = StructType([
        StructField("car_plate", StringType()),
        StructField("lane_id", IntegerType()),
        StructField("camera_id", IntegerType()),
        StructField("camera_index", IntegerType()),
        StructField("position_km", DoubleType()),
        StructField("speed_limit", DoubleType()),
        StructField("event_time", TimestampType()),
        StructField("speed_reading", DoubleType()),
    ])

    def make_events(spark, k: int, vehicles: int):
        """One k-camera journey per vehicle: sneaky at a constant 120 km/h between cameras."""
        rows = [
            (f"CAR {v:05d}", 1, i + 1, i, 1.0 + i, SPEED_LIMIT,
             T0 + timedelta(seconds=i * CROSSING_GAP_S), 80.0)   # legal AT each camera
            for v in range(vehicles)
            for i in range(k)
        ]
        return spark.createDataFrame(rows, schema)

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("join_compare")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Warm-up: run both strategies once on a tiny input so the first MEASURED run does not
    # absorb JVM/Spark session startup cost (without this, the first row reads ~0.5-1s high).
    warm = make_events(spark, 3, 10)
    for strategy in sorted(CAMERA_PAIR_CLAUSE):
        detect_average(warm, strategy).count()

    results: list[dict] = []
    for k in args.cameras:
        events = make_events(spark, k, args.vehicles).cache()
        events.count()  # materialise the input once so neither strategy pays generation cost
        for strategy in sorted(CAMERA_PAIR_CLAUSE):
            started = time.time()
            n = detect_average(events, strategy).count()
            elapsed = time.time() - started
            exp = expected_rows(strategy, k, args.vehicles)
            assert n == exp, f"k={k} {strategy}: got {n} rows, model says {exp}"
            results.append({
                "ran_at": datetime.utcnow().isoformat(),
                "cameras_k": k,
                "vehicles": args.vehicles,
                "events": args.vehicles * k,
                "strategy": strategy,
                "join_output_rows": n,
                "expected_rows": exp,
                "join_seconds": round(elapsed, 2),
            })
            print(f"[joincmp] k={k:<3d} {strategy:<9s} rows={n:<8d} (model {exp}) in {elapsed:.2f}s")
        events.unpersist()
    spark.stop()
    return results


def latest_by_k(csv_path: Path) -> dict[int, dict[str, dict]]:
    """Read the CSV and keep the most recent row per (k, strategy)."""
    by_k: dict[int, dict[str, dict]] = {}
    with csv_path.open() as f:
        for row in csv.DictReader(f):
            k = int(row["cameras_k"])
            by_k.setdefault(k, {})[row["strategy"]] = row
    return dict(sorted(by_k.items()))


def write_plot(csv_path: Path, png_path: Path) -> None:
    """Join output rows vs cameras-per-lane, one line per strategy, ratio annotated."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("[joincmp] matplotlib not installed — skipping plot (CSV is still written)")
        return

    by_k = latest_by_k(csv_path)
    ks = [k for k, pair in by_k.items() if {"adjacent", "all-pairs"} <= pair.keys()]
    if not ks:
        print("[joincmp] no complete (adjacent, all-pairs) pairs in the CSV — nothing to plot")
        return
    adj = [int(by_k[k]["adjacent"]["join_output_rows"]) for k in ks]
    allp = [int(by_k[k]["all-pairs"]["join_output_rows"]) for k in ks]
    vehicles = by_k[ks[0]]["adjacent"]["vehicles"]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.plot(ks, allp, "o-", color="#c0392b", linewidth=2, label=STRATEGY_LABEL["all-pairs"])
    ax.plot(ks, adj, "s-", color="#27ae60", linewidth=2, label=STRATEGY_LABEL["adjacent"])
    for k, a, b in zip(ks, adj, allp):
        ax.annotate(f"{b / a:.1f}× more rows", (k, b), textcoords="offset points",
                    xytext=(0, 10), ha="center", fontsize=9, color="#c0392b")
    ax.set_xlabel("Cameras per lane (k)")
    ax.set_ylabel(f"Join output rows ({vehicles} vehicles, each passing all k cameras)")
    ax.set_title("Join cost vs camera density — same violations detected, fewer join rows\n"
                 "(gap between the curves is surplus join work the A3 refinement avoids)",
                 fontsize=11)
    ax.set_xticks(ks)
    ax.set_ylim(top=max(allp) * 1.15)   # headroom so the top ratio annotation isn't clipped
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    fig.savefig(png_path, dpi=150)
    print(f"[joincmp] plot written to {png_path}")


def main() -> int:
    args = parse_args()
    out = Path(args.out)

    if not args.plot_only:
        results = run_sweep(args)
        is_new = not out.exists()
        with out.open("a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_HEADER)
            if is_new:
                w.writeheader()
            w.writerows(results)

        print(f"\n=== join-strategy comparison (appended to {out}) ===")
        print(f"  {'k':>3s}  {'adjacent O(k)':>14s}  {'all-pairs O(k²)':>15s}  {'baseline emits':>14s}")
        by_k = {k: {r["strategy"]: r for r in results if r["cameras_k"] == k} for k in args.cameras}
        for k, pair in by_k.items():
            adj, allp = pair["adjacent"]["join_output_rows"], pair["all-pairs"]["join_output_rows"]
            print(f"  {k:>3d}  {adj:>14d}  {allp:>15d}  {allp / adj:>10.1f}x rows")
        print("\n  adjacent  = A3 refinement (consecutive cameras only)   — the production default")
        print("  all-pairs = A2 baseline   (every camera pair)           — kept for this comparison")
        print("  Both flag IDENTICAL violations (proven by pipeline/verify_detect.py); the extra")
        print("  all-pairs rows are pure surplus join work, growing as k/2.")
    elif not out.exists():
        print(f"[joincmp] {out} does not exist — run the sweep first")
        return 1

    if not args.no_plot:
        write_plot(out, Path(args.png))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

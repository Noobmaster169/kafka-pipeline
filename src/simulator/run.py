"""
Traffic simulator runner.

Loads lanes/cameras/cars from MongoDB, generates trips at a configurable arrival rate
with a configurable behaviour mix, and streams enriched events to the single Kafka topic
(keyed by car_plate). Camera config is re-read periodically, so cameras an admin adds at
runtime start receiving traffic with no restart.

Run from src/:
    python -m simulator.run                          # live, default mix, 0.5 trips/s
    python -m simulator.run --rate 2 --total 200     # 2 trips/s, stop after 200 trips
    python -m simulator.run --normal 0.5 --speeder 0.3 --sneaky 0.2
    python -m simulator.run --fast --total 100000    # load-test: blast as fast as possible
    python -m simulator.run --source csv --scale 5   # replay data/camera_event_*.csv
    python -m simulator.run -v                        # DEBUG: log every trip, not just violations
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Make `common` and `simulator` importable when run as a plain script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402
from common.kafka_io import make_producer  # noqa: E402
from common.log import banner, get_logger  # noqa: E402
from common.mongo import get_db  # noqa: E402
from simulator.behaviors import (  # noqa: E402
    BEHAVIORS,
    generate_trip,
    pick_behavior,
    summarize_trip,
)

log = get_logger("sim")


def _label(summary: dict) -> str:
    """One word for what a trip is expected to trigger."""
    inst, avg = summary["expect_instantaneous"], summary["expect_average"]
    if inst and avg:
        return "INSTANT+AVG"
    if inst:
        return "INSTANT"
    if avg:
        return "AVERAGE"
    return "clean"


class Stats:
    """Running counters for periodic and final reporting."""

    def __init__(self) -> None:
        self.t0 = time.time()
        self.trips = 0
        self.sent = 0
        self.mix = {b: 0 for b in BEHAVIORS}
        self.expect_inst = 0
        self.expect_avg = 0

    def record_trip(self, behavior: str, summary: dict) -> None:
        self.trips += 1
        self.mix[behavior] += 1
        if summary["expect_instantaneous"]:
            self.expect_inst += 1
        if summary["expect_average"]:
            self.expect_avg += 1

    @property
    def elapsed(self) -> float:
        return max(time.time() - self.t0, 1e-9)

    @property
    def rate(self) -> float:
        return self.sent / self.elapsed

    def heartbeat(self, pending: int | None = None) -> str:
        m = self.mix
        tail = f"  pending={pending}" if pending is not None else ""
        return (f"stats   trips={self.trips} sent={self.sent} ({self.rate:.1f}/s)   "
                f"normal={m['normal']} speeder={m['speeder']} sneaky={m['sneaky']}{tail}")

    def final(self) -> str:
        return (f"done    trips={self.trips} sent={self.sent} in {self.elapsed:.1f}s "
                f"({self.rate:.1f}/s)   expected violations: instant={self.expect_inst} "
                f"average={self.expect_avg}")


def load_cameras_by_lane(db) -> dict[int, list[dict]]:
    """Group cameras by lane, each list ordered along the road by position."""
    by_lane: dict[int, list[dict]] = {}
    for cam in db[config.COLL_CAMERAS].find():
        by_lane.setdefault(int(cam["lane_id"]), []).append(cam)
    for cams in by_lane.values():
        cams.sort(key=lambda c: c["position_km"])
    return by_lane


def normalize_ratios(args) -> dict[str, float]:
    raw = {"normal": args.normal, "speeder": args.speeder, "sneaky": args.sneaky}
    total = sum(raw.values()) or 1.0
    return {k: v / total for k, v in raw.items()}


def show_banner(args, plates, cams_by_lane, ratios) -> None:
    """Print the boxed startup configuration."""
    rows = [("broker", config.KAFKA_BOOTSTRAP_SERVERS), ("topic", config.KAFKA_TOPIC)]
    if args.source == "synthetic":
        mode = f"synthetic{' / fast' if args.fast else ''} ({args.rate:g} trips/s)"
        mix = "   ".join(f"{k} {v * 100:.0f}%" for k, v in ratios.items())
        rows += [
            ("mode", mode),
            ("behaviour mix", mix),
            ("car pool", f"{len(plates)} cars"),
            ("stop after", f"{args.total} trips" if args.total else "unbounded"),
        ]
    else:
        rows.append(("mode", f"csv replay (scale {args.scale:g})"))
    rows.append(("cameras", f"refresh every {args.refresh:g}s"))
    for lid in sorted(cams_by_lane):
        layout = "  ".join(
            f"cam{c['camera_id']}@{c['position_km']:g}km/{c['speed_limit']:.0f}"
            for c in cams_by_lane[lid]
        )
        rows.append((f"lane {lid}", layout))
    banner("AWAS Traffic Simulator", rows)


def _log_trip(stats: Stats, behavior: str, plate: str, lane_id: int, summary: dict) -> None:
    """Log violation trips at INFO (the interesting ones); clean trips at DEBUG."""
    label = _label(summary)
    msg = ("trip #%-4d %-7s %-9s lane %d  -> %-11s (spot %.0f, avg %.0f)"
           % (stats.trips, behavior, plate, lane_id, label,
              summary["max_spot"], summary["max_avg"]))
    if label == "clean":
        log.debug(msg)
    else:
        log.info(msg)


# --------------------------------------------------------------------------- #
# Live mode — events sent at their scheduled wall-clock moment
# --------------------------------------------------------------------------- #
def run_live(args, producer, db, plates, rng, ratios) -> None:
    cams_by_lane = load_cameras_by_lane(db)
    lane_ids = sorted(cams_by_lane)
    interarrival = 1.0 / args.rate
    stats = Stats()

    heap: list[tuple[float, int, str, dict]] = []   # (send_epoch, seq, key, event)
    seq = itertools.count()

    now = time.time()
    next_arrival = now
    last_refresh = now
    last_stats = now
    last_snapshot = (0, 0)

    log.info("live mode — %g trips/s (Ctrl-C to stop)", args.rate)
    try:
        while True:
            now = time.time()

            # Pick up cameras added at runtime.
            if now - last_refresh >= args.refresh:
                before = {lid: len(c) for lid, c in cams_by_lane.items()}
                cams_by_lane = load_cameras_by_lane(db)
                lane_ids = sorted(cams_by_lane)
                after = {lid: len(c) for lid, c in cams_by_lane.items()}
                if after != before:
                    log.info("camera config changed: %s -> %s", before, after)
                last_refresh = now

            # Spawn any trips whose arrival time has come.
            while now >= next_arrival and (args.total is None or stats.trips < args.total):
                behavior = pick_behavior(rng, ratios)
                lane_id = rng.choice(lane_ids)
                plate = rng.choice(plates)
                start_time = datetime.fromtimestamp(next_arrival, tz=timezone.utc)
                events = generate_trip(plate, cams_by_lane[lane_id], behavior, start_time, rng)
                summary = summarize_trip(events)
                stats.record_trip(behavior, summary)
                _log_trip(stats, behavior, plate, lane_id, summary)
                for off, event in events:
                    heapq.heappush(heap, (next_arrival + off, next(seq), plate, event))
                next_arrival += interarrival

            # Send any events that are now due.
            while heap and heap[0][0] <= now:
                _, _, key, event = heapq.heappop(heap)
                producer.send(config.KAFKA_TOPIC, key=key, value=event)
                stats.sent += 1

            # Periodic heartbeat — only when something changed (no repeated spam).
            if now - last_stats >= args.stats_interval:
                snapshot = (stats.trips, stats.sent)
                if snapshot != last_snapshot:
                    log.info(stats.heartbeat(pending=len(heap)))
                    last_snapshot = snapshot
                last_stats = now

            # Done when all trips spawned and drained.
            if args.total is not None and stats.trips >= args.total and not heap:
                break

            # Sleep until the next thing happens (capped, so refresh stays responsive).
            now = time.time()
            wake = []
            if args.total is None or stats.trips < args.total:
                wake.append(next_arrival)
            if heap:
                wake.append(heap[0][0])
            time.sleep(max(0.0, min((min(wake) - now) if wake else 0.05, 0.1)))
    except KeyboardInterrupt:
        log.info("interrupted by user")
    finally:
        producer.flush()
        log.info(stats.final())


# --------------------------------------------------------------------------- #
# Fast mode — ignore timing, emit as fast as possible (for the load tests)
# --------------------------------------------------------------------------- #
def run_fast(args, producer, db, plates, rng, ratios) -> None:
    cams_by_lane = load_cameras_by_lane(db)
    lane_ids = sorted(cams_by_lane)
    base = datetime.now(timezone.utc).timestamp()
    stats = Stats()
    last_stats = time.time()

    log.info("fast mode — emitting as fast as possible (target %s trips)",
             args.total if args.total else "unbounded")
    while args.total is None or stats.trips < args.total:
        behavior = pick_behavior(rng, ratios)
        lane_id = rng.choice(lane_ids)
        plate = rng.choice(plates)
        # Spread trip starts on a synthetic clock so event_times stay distinct/ordered.
        start_time = datetime.fromtimestamp(base + stats.trips * 0.05, tz=timezone.utc)
        events = generate_trip(plate, cams_by_lane[lane_id], behavior, start_time, rng)
        stats.record_trip(behavior, summarize_trip(events))
        for _off, event in events:
            producer.send(config.KAFKA_TOPIC, key=plate, value=event)
            stats.sent += 1
        if time.time() - last_stats >= args.stats_interval:
            log.info(stats.heartbeat())
            last_stats = time.time()

    producer.flush()
    log.info(stats.final())


# --------------------------------------------------------------------------- #
# CSV mode — replay the A2 camera_event_*.csv files, enriched from camera config
# --------------------------------------------------------------------------- #
def run_csv(args, producer, db) -> None:
    import pandas as pd

    cams = {int(c["camera_id"]): c for c in db[config.COLL_CAMERAS].find()}
    data_dir = Path(__file__).resolve().parents[3] / "data"
    frames = []
    for letter in ("A", "B", "C"):
        path = data_dir / f"camera_event_{letter}.csv"
        if path.exists():
            df_part = pd.read_csv(path)
            log.info("loaded %s (%d rows)", path.name, len(df_part))
            frames.append(df_part)
    if not frames:
        log.error("no camera_event_*.csv under %s", data_dir)
        return
    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "camera_id", "speed_reading"]).sort_values("timestamp")
    if args.total:
        df = df.head(args.total)
    log.info("replaying %d events at scale %g wall-s per CSV-minute", len(df), args.scale)

    from common.schema import build_event

    sent = skipped = 0
    t0_sim = df["timestamp"].iloc[0]
    t0_wall = time.time()
    last_stats = t0_wall
    for _, row in df.iterrows():
        cam = cams.get(int(row["camera_id"]))
        if cam is None:
            skipped += 1
            continue
        target = t0_wall + (row["timestamp"] - t0_sim).total_seconds() / 60.0 * args.scale
        wait = target - time.time()
        if wait > 0:
            time.sleep(wait)
        event = build_event(
            car_plate=str(row["car_plate"]),
            lane_id=int(cam["lane_id"]),
            camera_id=int(cam["camera_id"]),
            position_km=float(cam["position_km"]),
            speed_limit=float(cam["speed_limit"]),
            timestamp=row["timestamp"].to_pydatetime(),
            speed_reading=float(row["speed_reading"]),
        )
        producer.send(config.KAFKA_TOPIC, key=event["car_plate"], value=event)
        sent += 1
        if time.time() - last_stats >= args.stats_interval:
            log.info("csv replay  sent=%d skipped=%d", sent, skipped)
            last_stats = time.time()
    producer.flush()
    log.info("done    csv replay sent=%d skipped(no-camera)=%d", sent, skipped)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AWAS synthetic traffic simulator.")
    p.add_argument("--source", choices=["synthetic", "csv"], default="synthetic")
    p.add_argument("--rate", type=float, default=0.5, help="Trips per second (live mode).")
    p.add_argument("--total", type=int, default=None, help="Stop after N trips/events.")
    p.add_argument("--fast", action="store_true", help="Blast events as fast as possible.")
    p.add_argument("--refresh", type=float, default=15.0,
                   help="Seconds between camera-config refreshes (hot-add support).")
    p.add_argument("--scale", type=float, default=10.0,
                   help="CSV mode: wall-seconds per CSV-minute.")
    p.add_argument("--normal", type=float, default=0.7)
    p.add_argument("--speeder", type=float, default=0.2)
    p.add_argument("--sneaky", type=float, default=0.1)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--stats-interval", type=float, default=5.0,
                   help="Seconds between periodic throughput summaries.")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="DEBUG logging: log every trip (not just violations).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)

    rng = random.Random(args.seed)
    db = get_db()
    producer = make_producer()

    if args.source == "csv":
        show_banner(args, [], load_cameras_by_lane(db), {})
        run_csv(args, producer, db)
        return 0

    plates = [d["car_plate"] for d in db[config.COLL_CARS].find({}, {"car_plate": 1, "_id": 0})]
    if not plates:
        log.error("no cars in MongoDB — run seed_db.py first.")
        return 1
    cams_by_lane = load_cameras_by_lane(db)
    if not cams_by_lane:
        log.error("no cameras in MongoDB — run seed_db.py first.")
        return 1
    ratios = normalize_ratios(args)
    show_banner(args, plates, cams_by_lane, ratios)

    if args.fast:
        run_fast(args, producer, db, plates, rng, ratios)
    else:
        run_live(args, producer, db, plates, rng, ratios)
    return 0


if __name__ == "__main__":
    sys.exit(main())

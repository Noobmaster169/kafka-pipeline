"""
Seed MongoDB with the reference data the platform needs before anything streams:
3 lanes, >= 3 cameras per lane, and the car pool (from data/vehicle.csv).

Run from the `src/` directory (so `common` is importable), or via
`deployment/scripts/seed.sh`:

    python seed_db.py                 # seed lanes + cameras + all cars
    python seed_db.py --cars-limit 500   # seed only the first 500 cars (faster demos)
    python seed_db.py --reset         # drop lanes/cameras/cars/violations first
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Make `common` importable when run as a plain script from anywhere.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402
from pymongo import ReplaceOne  # noqa: E402

from common import config  # noqa: E402
from common.log import get_logger  # noqa: E402
from common.mongo import ensure_indexes, get_db  # noqa: E402

log = get_logger("seed")

# Three lanes, each a stretch of highway with its own cameras.
DEFAULT_LANES = [
    {"lane_id": 1, "name": "North Highway"},
    {"lane_id": 2, "name": "Coastal Expressway"},
    {"lane_id": 3, "name": "City Bypass"},
]

# Per lane: how many cameras to seed and the limit they enforce (km/h).
LANE_CAMERA_PLAN = {
    1: {"count": 3, "limit": 90.0},
    2: {"count": 3, "limit": 100.0},
    3: {"count": 3, "limit": 80.0},
}

# data/vehicle.csv lives two levels up from src/ (repo root / data).
DEFAULT_VEHICLE_CSV = Path(__file__).resolve().parents[2] / "data" / "vehicle.csv"


def build_cameras() -> list[dict]:
    """Lay out cameras along each lane, 1 km apart, with globally-unique camera_ids."""
    cameras: list[dict] = []
    camera_id = 1
    for lane in DEFAULT_LANES:
        plan = LANE_CAMERA_PLAN[lane["lane_id"]]
        for i in range(plan["count"]):
            cameras.append({
                "camera_id": camera_id,
                "lane_id": lane["lane_id"],
                "position_km": round(config.CAMERA_SPACING_KM * (i + 1), 3),
                "speed_limit": float(plan["limit"]),
            })
            camera_id += 1
    return cameras


def _upsert(db, collection: str, key: str, docs: list[dict]) -> None:
    if not docs:
        return
    ops = [ReplaceOne({key: d[key]}, d, upsert=True) for d in docs]
    res = db[collection].bulk_write(ops, ordered=False)
    log.info("%-9s seeded: upserted=%d updated=%d (total=%d)",
             collection, res.upserted_count, res.modified_count,
             db[collection].count_documents({}))


def seed_cars(db, csv_path: Path, limit: int | None) -> None:
    """Load the car pool from vehicle.csv into the `cars` collection."""
    log.info("reading car pool from %s ...", csv_path)
    df = pd.read_csv(csv_path)
    log.info("vehicle.csv has %d rows%s", len(df),
             f"; seeding first {limit}" if limit else "; seeding all")
    if limit:
        df = df.head(limit)
    docs = [
        {
            "car_plate": str(r["car_plate"]),
            "owner_name": str(r["owner_name"]),
            "owner_addr": str(r["owner_addr"]),
            # Source CSV header has a typo ("vechicle_type"); normalise it here.
            "vehicle_type": str(r["vechicle_type"]),
            "registration_date": str(r["registration_date"]),
        }
        for _, r in df.iterrows()
    ]
    _upsert(db, config.COLL_CARS, "car_plate", docs)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed AWAS reference data into MongoDB.")
    p.add_argument("--reset", action="store_true",
                   help="Drop lanes/cameras/cars/violations before seeding.")
    p.add_argument("--cars-limit", type=int, default=None,
                   help="Seed only the first N cars (default: all).")
    p.add_argument("--vehicle-csv", type=Path, default=DEFAULT_VEHICLE_CSV,
                   help=f"Path to vehicle.csv (default: {DEFAULT_VEHICLE_CSV}).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()
    log.info("seeding AWAS reference data -> %s:%s/%s",
             config.MONGO_HOST, config.MONGO_PORT, config.MONGO_DB)

    db = get_db()
    if args.reset:
        for coll in (config.COLL_LANES, config.COLL_CAMERAS,
                     config.COLL_CARS, config.COLL_VIOLATIONS):
            db.drop_collection(coll)
            log.info("dropped collection %s", coll)

    ensure_indexes()

    _upsert(db, config.COLL_LANES, "lane_id", DEFAULT_LANES)
    cameras = build_cameras()
    _upsert(db, config.COLL_CAMERAS, "camera_id", cameras)
    # Show the seeded camera layout so the road geometry is visible in the logs.
    for lane in DEFAULT_LANES:
        layout = ", ".join(
            f"cam{c['camera_id']}@{c['position_km']}km/{c['speed_limit']:.0f}"
            for c in cameras if c["lane_id"] == lane["lane_id"]
        )
        log.info("  lane %d (%s): %s", lane["lane_id"], lane["name"], layout)

    seed_cars(db, args.vehicle_csv, args.cars_limit)

    log.info(
        "done in %.2fs — lanes=%d cameras=%d cars=%d",
        time.time() - started,
        db[config.COLL_LANES].count_documents({}),
        db[config.COLL_CAMERAS].count_documents({}),
        db[config.COLL_CARS].count_documents({}),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

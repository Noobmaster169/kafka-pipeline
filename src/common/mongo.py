"""
MongoDB access for AWAS: a lazily-opened client, the collection handles, and the
index set every service relies on.

Collections
-----------
- lanes      : { lane_id, name }
- cameras    : { camera_id, lane_id, position_km, speed_limit }
- cars       : { car_plate, owner_name, owner_addr, vehicle_type, registration_date }
- violations : one document per detected violation (see ensure_indexes for the key)
"""

from __future__ import annotations

from pymongo import ASCENDING, MongoClient
from pymongo.database import Database

from common import config
from common.log import get_logger

log = get_logger("mongo")

_client: MongoClient | None = None


def get_db() -> Database:
    """Return the AWAS database, opening the client on first use (cached thereafter)."""
    global _client
    if _client is None:
        uri = f"{config.MONGO_HOST}:{config.MONGO_PORT}/{config.MONGO_DB}"
        log.debug("connecting to mongodb %s ...", uri)
        # 5 s selection timeout surfaces a missing/misconfigured server fast at startup
        # instead of pymongo's 30 s default hang.
        _client = MongoClient(
            host=config.MONGO_HOST,
            port=config.MONGO_PORT,
            serverSelectionTimeoutMS=5000,
        )
        try:
            version = _client.admin.command("buildInfo").get("version", "?")
            log.info("MongoDB %s connected (%s)", version, uri)
        except Exception as exc:  # surface unreachable server with a clear message
            log.error("cannot reach MongoDB at %s: %s", uri, exc)
            raise
    return _client[config.MONGO_DB]


def ensure_indexes() -> Database:
    """Create every index the platform depends on (idempotent — safe to re-run)."""
    db = get_db()
    log.debug("ensuring indexes on lanes/cameras/cars/violations ...")

    db[config.COLL_LANES].create_index("lane_id", unique=True)

    db[config.COLL_CAMERAS].create_index("camera_id", unique=True)
    # Lets us fetch a lane's cameras already ordered along the road.
    db[config.COLL_CAMERAS].create_index(
        [("lane_id", ASCENDING), ("position_km", ASCENDING)]
    )

    db[config.COLL_CARS].create_index("car_plate", unique=True)

    violations = db[config.COLL_VIOLATIONS]
    # Query indexes for the dashboard / tracking page.
    violations.create_index("car_plate")
    violations.create_index("lane_id")
    violations.create_index("date")
    # Idempotency key: one violation per vehicle per detection window. `window_start` is
    # the start time floored to DEDUP_WINDOW, matching the pipeline's car_plate-keyed dedup
    # grain — so a replayed micro-batch upserts onto the same document instead of inserting
    # a duplicate, even across restarts.
    violations.create_index(
        [
            ("car_plate", ASCENDING),
            ("window_start", ASCENDING),
        ],
        unique=True,
        name="uniq_violation",
    )
    log.info(
        "indexes ready — lanes=%d cameras=%d cars=%d violations=%d",
        db[config.COLL_LANES].count_documents({}),
        db[config.COLL_CAMERAS].count_documents({}),
        db[config.COLL_CARS].count_documents({}),
        db[config.COLL_VIOLATIONS].count_documents({}),
    )
    return db

"""
Data access for the backend — every MongoDB read and write the API performs.

Routers stay thin by delegating here; this module is the only place that knows the
collection shapes (`common.mongo`) and returns JSON-safe dicts (`serialize.jsonify`).
Nothing here raises HTTP errors — callers translate `None` / `ValueError` /
`DuplicateKeyError` into the right status code.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timedelta
from typing import Iterator

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import DESCENDING

from backend.serialize import jsonify
from common import config
from common.mongo import get_db

# Column order for the violations CSV export (one row per violation).
CSV_FIELDS = [
    "car_plate", "lane_id", "violation_type",
    "camera_id_start", "camera_id_end",
    "position_start_km", "position_end_km",
    "timestamp_start", "timestamp_end",
    "speed_limit", "speed_reading", "avg_speed",
    "date", "detected_at",
]


# --------------------------------------------------------------------------- #
# Lanes
# --------------------------------------------------------------------------- #
def _violation_counts_by_lane() -> dict[int, dict]:
    """One pass over `violations`, grouped per lane into total/instant/average counts."""
    pipeline = [
        {"$group": {
            "_id": {"lane_id": "$lane_id", "violation_type": "$violation_type"},
            "count": {"$sum": 1},
        }}
    ]
    by_lane: dict[int, dict] = {}
    for row in get_db()[config.COLL_VIOLATIONS].aggregate(pipeline):
        lane_id = row["_id"]["lane_id"]
        vtype = row["_id"]["violation_type"]
        bucket = by_lane.setdefault(lane_id, {"total": 0, "instantaneous": 0, "average": 0})
        bucket["total"] += row["count"]
        if vtype == "INSTANTANEOUS":
            bucket["instantaneous"] += row["count"]
        elif vtype == "AVERAGE":
            bucket["average"] += row["count"]
    return by_lane


def _camera_counts_by_lane() -> dict[int, int]:
    pipeline = [{"$group": {"_id": "$lane_id", "count": {"$sum": 1}}}]
    return {r["_id"]: r["count"] for r in get_db()[config.COLL_CAMERAS].aggregate(pipeline)}


def list_lanes_with_summary() -> list[dict]:
    """Every lane with its camera count and violation tallies — the dashboard overview."""
    db = get_db()
    violations = _violation_counts_by_lane()
    cameras = _camera_counts_by_lane()
    out = []
    for lane in db[config.COLL_LANES].find().sort("lane_id"):
        lane_id = lane["lane_id"]
        doc = jsonify(lane)
        doc["camera_count"] = cameras.get(lane_id, 0)
        doc["violations"] = violations.get(
            lane_id, {"total": 0, "instantaneous": 0, "average": 0}
        )
        out.append(doc)
    return out


def get_lane(lane_id: int) -> dict | None:
    """One lane with its cameras (ordered along the road) and violation summary."""
    db = get_db()
    lane = db[config.COLL_LANES].find_one({"lane_id": lane_id})
    if lane is None:
        return None
    doc = jsonify(lane)
    doc["cameras"] = list_cameras(lane_id)
    doc["violations"] = _violation_counts_by_lane().get(
        lane_id, {"total": 0, "instantaneous": 0, "average": 0}
    )
    return doc


# --------------------------------------------------------------------------- #
# Cameras
# --------------------------------------------------------------------------- #
def list_cameras(lane_id: int | None = None) -> list[dict]:
    """All cameras, or one lane's, ordered by position along the road."""
    query = {} if lane_id is None else {"lane_id": lane_id}
    cursor = get_db()[config.COLL_CAMERAS].find(query).sort([
        ("lane_id", 1), ("position_km", 1)
    ])
    return [jsonify(c) for c in cursor]


def append_camera(lane_id: int, speed_limit: float | None = None) -> dict:
    """
    Append a camera to the end of a lane (proposal D3 — runtime-extensible).

    The new camera sits CAMERA_SPACING_KM past the lane's current last camera, takes
    the next globally-unique camera_id, and inherits the lane's limit unless one is
    given. The running simulator re-reads camera config periodically, so the new
    camera starts receiving traffic with no pipeline restart.
    """
    db = get_db()
    if db[config.COLL_LANES].find_one({"lane_id": lane_id}) is None:
        raise ValueError(f"lane {lane_id} does not exist")

    lane_cams = list(db[config.COLL_CAMERAS].find({"lane_id": lane_id}))
    if lane_cams:
        last_position = max(c["position_km"] for c in lane_cams)
        inherited_limit = max(c["speed_limit"] for c in lane_cams)
    else:
        last_position = 0.0
        inherited_limit = 100.0  # only hit for an empty lane; a sane default limit

    max_id = db[config.COLL_CAMERAS].find_one(sort=[("camera_id", DESCENDING)])
    next_id = (max_id["camera_id"] + 1) if max_id else 1

    camera = {
        "camera_id": next_id,
        "lane_id": lane_id,
        "position_km": round(last_position + config.CAMERA_SPACING_KM, 3),
        "speed_limit": float(speed_limit if speed_limit is not None else inherited_limit),
    }
    db[config.COLL_CAMERAS].insert_one(camera)
    return jsonify(camera)


# --------------------------------------------------------------------------- #
# Cars
# --------------------------------------------------------------------------- #
def search_cars(plate: str | None, skip: int, limit: int) -> dict:
    """Paginated car list, optionally narrowed by a case-insensitive plate prefix."""
    query: dict = {}
    if plate:
        # Anchored, escaped prefix match so the plate text is treated literally.
        query["car_plate"] = {"$regex": f"^{_escape_regex(plate)}", "$options": "i"}
    coll = get_db()[config.COLL_CARS]
    total = coll.count_documents(query)
    cursor = coll.find(query).sort("car_plate", 1).skip(skip).limit(limit)
    return {"total": total, "items": [jsonify(c) for c in cursor]}


def get_car_with_violations(plate: str) -> dict | None:
    """A car plus its violations (newest first) — the car-detail view."""
    db = get_db()
    car = db[config.COLL_CARS].find_one({"car_plate": plate})
    if car is None:
        return None
    doc = jsonify(car)
    cursor = db[config.COLL_VIOLATIONS].find({"car_plate": plate}).sort(
        "detected_at", DESCENDING
    )
    doc["violations"] = [jsonify(v) for v in cursor]
    return doc


def create_car(body: dict) -> dict:
    """Insert a new car. Raises pymongo.errors.DuplicateKeyError on an existing plate."""
    get_db()[config.COLL_CARS].insert_one(dict(body))
    return jsonify(get_db()[config.COLL_CARS].find_one({"car_plate": body["car_plate"]}))


# --------------------------------------------------------------------------- #
# Violations
# --------------------------------------------------------------------------- #
def _violation_filter(
    lane_id: int | None,
    violation_type: str | None,
    car_plate: str | None,
    date: str | None,
) -> dict:
    """Build the Mongo query for the violations list / export from optional filters."""
    query: dict = {}
    if lane_id is not None:
        query["lane_id"] = lane_id
    if violation_type:
        query["violation_type"] = violation_type
    if car_plate:
        query["car_plate"] = car_plate
    if date:
        # `date` is stored as the start-day midnight; match the whole day [d, d+1).
        day = datetime.fromisoformat(date)
        day = day.replace(hour=0, minute=0, second=0, microsecond=0)
        query["date"] = {"$gte": day, "$lt": day + timedelta(days=1)}
    return query


def query_violations(
    *,
    lane_id: int | None = None,
    violation_type: str | None = None,
    car_plate: str | None = None,
    date: str | None = None,
    skip: int = 0,
    limit: int = 50,
) -> dict:
    """Filtered, paginated violations, newest detection first; with a total count."""
    query = _violation_filter(lane_id, violation_type, car_plate, date)
    coll = get_db()[config.COLL_VIOLATIONS]
    total = coll.count_documents(query)
    cursor = coll.find(query).sort("detected_at", DESCENDING).skip(skip).limit(limit)
    return {"total": total, "items": [jsonify(v) for v in cursor]}


def get_violation(violation_id: str) -> dict | None:
    """One violation by its Mongo id; None if the id is malformed or absent."""
    try:
        oid = ObjectId(violation_id)
    except InvalidId:
        return None
    doc = get_db()[config.COLL_VIOLATIONS].find_one({"_id": oid})
    return jsonify(doc) if doc else None


def iter_violations_csv(
    *,
    lane_id: int | None = None,
    violation_type: str | None = None,
    car_plate: str | None = None,
    date: str | None = None,
) -> Iterator[str]:
    """Yield the violations matching the filters as CSV text, header first, row by row."""
    query = _violation_filter(lane_id, violation_type, car_plate, date)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDS, extrasaction="ignore")

    writer.writeheader()
    yield _drain(buffer)

    cursor = get_db()[config.COLL_VIOLATIONS].find(query).sort("detected_at", DESCENDING)
    for doc in cursor:
        writer.writerow(jsonify(doc))
        yield _drain(buffer)


def _drain(buffer: io.StringIO) -> str:
    """Return everything written to the buffer so far and reset it."""
    text = buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    return text


def _escape_regex(text: str) -> str:
    """Escape regex metacharacters so a plate query is matched literally."""
    import re

    return re.escape(text)

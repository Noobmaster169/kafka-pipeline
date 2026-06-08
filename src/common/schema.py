"""
The camera-detection event — the one message shape that flows through Kafka.

A3 enriches each event at the source with `lane_id`, `position_km`, and
`speed_limit` (decision D3 in the proposal). This is what lets the Spark self-join
compute segment distance and the legal limit from the two joined rows alone, with
no camera-config lookup inside Spark and no restart when a camera is added.
"""

from __future__ import annotations

import uuid
from datetime import datetime

# The field set of the JSON payload published to Kafka (key = car_plate).
EVENT_FIELDS = [
    "event_id",
    "car_plate",
    "lane_id",
    "camera_id",
    "position_km",
    "speed_limit",
    "timestamp",      # ISO-8601 string; Spark parses this into event_time
    "speed_reading",
]


def build_event(
    *,
    car_plate: str,
    lane_id: int,
    camera_id: int,
    position_km: float,
    speed_limit: float,
    timestamp,
    speed_reading: float,
    event_id: str | None = None,
) -> dict:
    """Construct one enriched event payload as a plain dict (ready to JSON-serialise)."""
    ts = timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp)
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "car_plate": str(car_plate),
        "lane_id": int(lane_id),
        "camera_id": int(camera_id),
        "position_km": float(position_km),
        "speed_limit": float(speed_limit),
        "timestamp": ts,
        "speed_reading": float(speed_reading),
    }


def spark_event_schema():
    """
    Build the PySpark StructType matching `EVENT_FIELDS`.

    pyspark is imported lazily so non-Spark services (the simulator, the backend)
    can import this module without having pyspark installed.
    """
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    return StructType([
        StructField("event_id",      StringType()),
        StructField("car_plate",     StringType()),
        StructField("lane_id",       IntegerType()),
        StructField("camera_id",     IntegerType()),
        StructField("position_km",   DoubleType()),
        StructField("speed_limit",   DoubleType()),
        StructField("timestamp",     StringType()),
        StructField("speed_reading", DoubleType()),
    ])

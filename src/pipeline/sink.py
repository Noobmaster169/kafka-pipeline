"""
The streaming sink: a `foreachBatch` handler that, for each micro-batch of detected
violations, (1) upserts one document per violation into MongoDB (idempotent across
restarts via the unique key) and (2) republishes each violation to the `violations`
Kafka topic so the backend can stream a live log without MongoDB change streams.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pymongo import UpdateOne

from common import config
from common.kafka_io import make_producer
from common.log import get_logger
from common.mongo import get_db

log = get_logger("pipeline")

# The per-car, per-window idempotency key (matches the unique Mongo index): one stored
# violation per vehicle per DEDUP_WINDOW, regardless of type.
_KEY_FIELDS = ("car_plate", "window_start")


def _window_start(ts: datetime) -> datetime:
    """Floor an event time to the DEDUP_WINDOW bucket — the dedup grain at rest.

    `(car_plate, window_start)` is the idempotency key. It matches the pipeline's
    "one violation per car per DEDUP_WINDOW" dedup, so a replayed micro-batch upserts
    onto the same document instead of inserting a duplicate — even if it re-emits a
    different representative crossing for the same car-window.
    """
    w = config.DEDUP_WINDOW_MINUTES
    floored = (ts.minute // w) * w if 0 < w <= 60 else 0
    return ts.replace(minute=floored, second=0, microsecond=0)


def _to_document(row) -> dict:
    """Convert a unified violation Row into the stored MongoDB document."""
    ts_start: datetime = row["timestamp_start"]
    ts_end: datetime = row["timestamp_end"]
    return {
        "car_plate": row["car_plate"],
        "lane_id": int(row["lane_id"]),
        "violation_type": row["violation_type"],
        "camera_id_start": int(row["camera_id_start"]),
        "camera_id_end": int(row["camera_id_end"]),
        "position_start_km": float(row["position_start_km"]),
        "position_end_km": float(row["position_end_km"]),
        "timestamp_start": ts_start,
        "timestamp_end": ts_end,
        "speed_limit": float(row["speed_limit"]),
        "speed_reading": None if row["speed_reading"] is None else float(row["speed_reading"]),
        "avg_speed": None if row["avg_speed"] is None else float(row["avg_speed"]),
        # Per-car idempotency window (start floored to DEDUP_WINDOW) + daily bucket +
        # when we detected it.
        "window_start": _window_start(ts_start),
        "date": ts_start.replace(hour=0, minute=0, second=0, microsecond=0),
        "detected_at": datetime.now(timezone.utc),
    }


def _to_json(doc: dict) -> dict:
    """A JSON-safe copy (datetimes -> ISO strings) for publishing to Kafka."""
    out = dict(doc)
    for field in ("timestamp_start", "timestamp_end", "window_start", "date", "detected_at"):
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    return out


class ViolationSink:
    """Stateful `foreachBatch` callable holding the Mongo + Kafka handles."""

    def __init__(self) -> None:
        self.collection = get_db()[config.COLL_VIOLATIONS]
        self.producer = make_producer()

    def __call__(self, batch_df, batch_id: int) -> None:
        rows = batch_df.collect()
        if not rows:
            log.debug("batch %-3d  0 violations", batch_id)
            return

        ops: list[UpdateOne] = []
        counts = {"INSTANTANEOUS": 0, "AVERAGE": 0}
        for row in rows:
            doc = _to_document(row)
            counts[doc["violation_type"]] += 1
            # $setOnInsert keyed on the unique fields = idempotent one-doc-per-violation.
            ops.append(UpdateOne(
                {k: doc[k] for k in _KEY_FIELDS},
                {"$setOnInsert": doc},
                upsert=True,
            ))
            self.producer.send(config.KAFKA_VIOLATIONS_TOPIC, key=doc["car_plate"], value=_to_json(doc))

        result = self.collection.bulk_write(ops, ordered=False)
        self.producer.flush()
        new = result.upserted_count
        log.info(
            "batch %-3d  violations=%-3d (instant=%d average=%d)  new=%d duplicate=%d  -> mongo+kafka",
            batch_id, len(rows), counts["INSTANTANEOUS"], counts["AVERAGE"], new, len(rows) - new,
        )
        for row in rows:
            speed = row["avg_speed"] if row["violation_type"] == "AVERAGE" else row["speed_reading"]
            log.debug(
                "    %-13s %-9s lane %d cam %d->%d  %.0f km/h (limit %.0f)",
                row["violation_type"], row["car_plate"], row["lane_id"],
                row["camera_id_start"], row["camera_id_end"], speed or 0.0, row["speed_limit"],
            )

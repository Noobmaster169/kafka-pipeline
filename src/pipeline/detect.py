"""
Violation detection — the pure DataFrame transforms.

These functions are written so they work on *either* a streaming DataFrame (the live
pipeline) or a static one (the unit test), which lets us verify the join logic in batch
mode without a running Kafka broker. The implementation follows section 4.2 of
A3_PROPOSAL.md:

  - INSTANTANEOUS: a stateless per-event filter (speed_reading > speed_limit).
  - AVERAGE     : one generalised self-join of the stream with itself, matching a vehicle's
                  crossings at two different cameras on the same lane within the window,
                  then computing the segment average from the two joined rows alone.

Every detector emits the same unified column set so the two streams can be unioned.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, from_json, lit, to_timestamp, unix_timestamp

from common import config
from common.schema import spark_event_schema

# The shared shape of a violation row (after either detector).
UNIFIED_COLUMNS = [
    "car_plate", "lane_id", "violation_type",
    "camera_id_start", "camera_id_end",
    "position_start_km", "position_end_km",
    "timestamp_start", "timestamp_end",
    "speed_limit", "speed_reading", "avg_speed",
]


def parse_events(raw: DataFrame) -> DataFrame:
    """Decode the Kafka `value` JSON into typed columns + event_time + watermark."""
    return (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), spark_event_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", config.WATERMARK_DURATION)
    )


def read_events(spark) -> DataFrame:
    """Subscribe to the single camera-events topic and parse it."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    return parse_events(raw)


def detect_instantaneous(events: DataFrame) -> DataFrame:
    """Rule 1 — a single camera reading over that camera's own limit. Stateless."""
    return (
        events.filter(col("speed_reading") > col("speed_limit"))
        .select(
            col("car_plate"),
            col("lane_id"),
            lit("INSTANTANEOUS").alias("violation_type"),
            col("camera_id").alias("camera_id_start"),
            col("camera_id").alias("camera_id_end"),
            col("position_km").alias("position_start_km"),
            col("position_km").alias("position_end_km"),
            col("event_time").alias("timestamp_start"),
            col("event_time").alias("timestamp_end"),
            col("speed_limit"),
            col("speed_reading"),
            lit(None).cast("double").alias("avg_speed"),
        )
    )


def detect_average(events: DataFrame) -> DataFrame:
    """
    Rule 2 — the generalised self-join (proposal §4.2).

    Pair every event `a` with a later event `b` of the same vehicle, on the same lane, at a
    different camera, within the journey window; the average speed is computed from the two
    rows' positions and times alone (no camera-config lookup — proposal D3).
    """
    a = events.alias("a")
    b = events.alias("b")

    condition = expr(f"""
        a.car_plate  =  b.car_plate                                AND
        a.lane_id    =  b.lane_id                                  AND
        a.camera_id  <> b.camera_id                                AND
        b.event_time >  a.event_time                               AND
        b.event_time <= a.event_time + interval {config.JOIN_WINDOW}
    """)

    paired = a.join(b, condition, "inner").select(
        col("a.car_plate").alias("car_plate"),
        col("a.lane_id").alias("lane_id"),
        col("a.camera_id").alias("camera_id_start"),
        col("b.camera_id").alias("camera_id_end"),
        col("a.position_km").alias("position_start_km"),
        col("b.position_km").alias("position_end_km"),
        col("a.event_time").alias("timestamp_start"),
        col("b.event_time").alias("timestamp_end"),
        col("b.speed_limit").alias("speed_limit"),   # end-camera limit governs the segment
        F.abs(col("b.position_km") - col("a.position_km")).alias("distance_km"),
        (unix_timestamp("b.event_time") - unix_timestamp("a.event_time")).alias("dt_seconds"),
    )

    with_avg = (
        paired.filter(col("dt_seconds") > 0)
        .withColumn("avg_speed", col("distance_km") * 3600.0 / col("dt_seconds"))
    )

    return with_avg.filter(col("avg_speed") > col("speed_limit")).select(
        col("car_plate"),
        col("lane_id"),
        lit("AVERAGE").alias("violation_type"),
        col("camera_id_start"),
        col("camera_id_end"),
        col("position_start_km"),
        col("position_end_km"),
        col("timestamp_start"),
        col("timestamp_end"),
        col("speed_limit"),
        lit(None).cast("double").alias("speed_reading"),
        col("avg_speed"),
    )


def build_violations(events: DataFrame) -> DataFrame:
    """
    Combine both detectors and collapse repeated detections of the same offence.

    De-dup keys on (car_plate, violation_type) within DEDUP_WINDOW — this is the fix for
    the A2 bug (which keyed on car_plate alone and dropped a vehicle's second violation
    type). A single drive that fires the same type at several cameras/segments collapses to
    one row; a different type is kept.
    """
    instantaneous = detect_instantaneous(events)
    average = detect_average(events)
    combined = instantaneous.unionByName(average)
    return (
        combined.withWatermark("timestamp_start", config.DEDUP_WINDOW)
        .dropDuplicates(["car_plate", "violation_type"])
    )

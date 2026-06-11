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


# The two camera-pair predicates the join-strategy comparison measures (REPORT.md §5.2/§8).
# The cost model of the stream-join literature (Kang et al.; Teubner & Mueller's handshake
# join) prices a windowed join by its candidate tuple-pair comparisons, so the strategies
# differ exactly in the candidate space they admit per k-camera journey:
#   adjacent  — |Δcamera_index| = 1 : k-1 = O(k) pairs (A3's consecutive-segment refinement)
#   all-pairs — any distinct pair   : k(k-1)/2 = O(k²) pairs (A2's semantics; the general
#               windowed-join formulation, kept as the measured baseline)
CAMERA_PAIR_CLAUSE = {
    "adjacent": "abs(b.camera_index - a.camera_index) = 1",
    "all-pairs": "a.camera_index <> b.camera_index",
}


def detect_average(events: DataFrame, strategy: str | None = None) -> DataFrame:
    """
    Rule 2 — the generalised self-join (proposal §4.2, consecutive-segment refinement).

    Pair every event `a` with a later event `b` of the same vehicle, on the same lane, at a
    second camera chosen by `strategy` (default `config.JOIN_STRATEGY` = adjacent), within
    the journey window; the average speed is computed from the two rows' positions and times
    alone (no camera-config lookup — D3).

    Pairing only adjacent cameras (rather than every camera pair) computes the average over
    consecutive segments X-1 -> X only, never across a skipped camera (X-2). This drops the
    per-vehicle output from O(k^2) to O(k) pairs while still catching the sneaky driver (a
    constant between-camera speed makes every consecutive segment exceed the limit). The
    |Δindex| = 1 form (not index+1) keeps both travel directions; `b.event_time > a.event_time`
    then fixes `a` as the earlier crossing so each pair yields exactly one row.

    The `all-pairs` strategy swaps clause (iii) for `camera_index <> camera_index` — A2's
    behaviour — and exists so `verify_detect.py` and `benchmarks/join_compare.py` can measure
    the two strategies against each other on identical input.

    Trade-off (adjacent): a *missed* camera reading breaks the index chain for that gap (cam0
    then cam2 won't pair), the price of doing this without arbitrary stateful processing
    (unavailable in PySpark 3.3).
    """
    strategy = strategy or config.JOIN_STRATEGY
    if strategy not in CAMERA_PAIR_CLAUSE:
        raise ValueError(f"unknown JOIN_STRATEGY {strategy!r}; expected one of {sorted(CAMERA_PAIR_CLAUSE)}")

    a = events.alias("a")
    b = events.alias("b")

    condition = expr(f"""
        a.car_plate  =  b.car_plate                                AND
        a.lane_id    =  b.lane_id                                  AND
        {CAMERA_PAIR_CLAUSE[strategy]}                             AND
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


def build_violations(events: DataFrame, strategy: str | None = None) -> DataFrame:
    """
    Combine both detectors and collapse to one violation per car per window.

    A speeding car fires many raw detections — INSTANTANEOUS at several cameras and AVERAGE
    on several segments. For enforcement we only need to flag the car once, so the de-dup
    keys on `car_plate` alone within DEDUP_WINDOW: the first violation seen for a plate (of
    either type) is emitted and the rest are dropped. The surviving type is not significant
    and is simply whichever row arrives first.

    This is a rolling, watermark-bounded window, NOT permanent suppression. A plate is held
    in dedup state only for DEDUP_WINDOW of event time, then evicted — so a later offence by
    the same car is recorded again. A car can therefore be flagged many times across a
    day/hour, but at most once per ~DEDUP_WINDOW.
    """
    instantaneous = detect_instantaneous(events)
    average = detect_average(events, strategy)
    combined = instantaneous.unionByName(average)
    return (
        combined.withWatermark("timestamp_start", config.DEDUP_WINDOW)
        .dropDuplicates(["car_plate"])
    )

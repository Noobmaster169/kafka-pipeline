"""
Batch verification of the detection logic — runs in local Spark with NO Kafka broker.

Because the transforms in `detect.py` work on static DataFrames too, we can feed a fixed
scenario (one normal, one speeder, one sneaky driver) and assert the pipeline classifies
each correctly. This is the offline proof that the generalised self-join (proposal §4.2)
behaves as designed.

    python -m pipeline.verify_detect
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from pipeline.detect import build_violations, detect_average, detect_instantaneous  # noqa: E402

T0 = datetime(2024, 1, 1, 8, 0, 0)
EVENT_SCHEMA = StructType([
    StructField("car_plate", StringType()),
    StructField("lane_id", IntegerType()),
    StructField("camera_id", IntegerType()),
    StructField("position_km", DoubleType()),
    StructField("speed_limit", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("speed_reading", DoubleType()),
])

# Lane 1: cameras 1/2/3 at 1/2/3 km, limit 90.  (plate, cam, pos, limit, +seconds, reading)
ROWS = [
    # NORMAL — steady 72 km/h: under the limit at every camera AND on average.
    ("NRM 1", 1, 1, 1.0, 90.0, T0 + timedelta(seconds=0),   80.0),
    ("NRM 1", 1, 2, 2.0, 90.0, T0 + timedelta(seconds=50),  82.0),
    ("NRM 1", 1, 3, 3.0, 90.0, T0 + timedelta(seconds=100), 79.0),
    # SPEEDER — over the limit everywhere; ~144 km/h average.
    ("SPD 1", 1, 1, 1.0, 90.0, T0 + timedelta(seconds=0),   130.0),
    ("SPD 1", 1, 2, 2.0, 90.0, T0 + timedelta(seconds=25),  132.0),
    ("SPD 1", 1, 3, 3.0, 90.0, T0 + timedelta(seconds=50),  128.0),
    # SNEAKY — UNDER the limit at every camera, but ~120 km/h average between them.
    ("SNK 1", 1, 1, 1.0, 90.0, T0 + timedelta(seconds=0),   80.0),
    ("SNK 1", 1, 2, 2.0, 90.0, T0 + timedelta(seconds=30),  82.0),
    ("SNK 1", 1, 3, 3.0, 90.0, T0 + timedelta(seconds=60),  79.0),
]


def main() -> int:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("verify_detect")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    events = spark.createDataFrame(ROWS, EVENT_SCHEMA)

    inst = {(r["car_plate"]) for r in detect_instantaneous(events).collect()}
    avg = [r for r in detect_average(events).collect()]
    avg_plates = sorted(r["car_plate"] for r in avg)
    final = sorted(((r["car_plate"], r["violation_type"]) for r in build_violations(events).collect()))

    print("\n--- raw detector output ---")
    print(f"instantaneous plates : {sorted(inst)}")
    print(f"average rows         : {len(avg)} (plates {avg_plates})")
    print("\n--- final de-duplicated violations ---")
    for plate, vtype in final:
        print(f"  {plate:6s} {vtype}")

    # Expected semantics (the whole point of the system).
    assert sorted(inst) == ["SPD 1"], f"instantaneous wrong: {sorted(inst)}"
    assert avg_plates == ["SNK 1", "SNK 1", "SNK 1", "SPD 1", "SPD 1", "SPD 1"], avg_plates
    assert final == [("SNK 1", "AVERAGE"), ("SPD 1", "AVERAGE"), ("SPD 1", "INSTANTANEOUS")], final

    print("\nPASS — normal=clean, speeder=INSTANT+AVG, sneaky=AVERAGE-only; "
          "3 average segments per car collapse to one violation.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

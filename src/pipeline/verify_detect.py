"""
Batch verification of the detection logic — runs in local Spark with NO Kafka broker.

Because the transforms in `detect.py` work on static DataFrames too, we can feed a fixed
scenario (one normal, one speeder, one sneaky driver) and assert the pipeline classifies
each correctly. This is the offline proof that the generalised self-join (proposal §4.2)
behaves as designed.

It also proves the join-strategy equivalence claim of REPORT.md §5.2: the `adjacent`
refinement and the `all-pairs` baseline (A2's semantics) are run on the SAME fixture, and
we assert (a) all-pairs emits k(k-1)/2 = 3 raw pairs per violator vs adjacent's k-1 = 2,
and (b) both de-duplicate to the IDENTICAL final violations — same enforcement power,
strictly smaller join output.

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
    StructField("camera_index", IntegerType()),
    StructField("position_km", DoubleType()),
    StructField("speed_limit", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("speed_reading", DoubleType()),
])

# Lane 1: cameras 1/2/3 (index 0/1/2) at 1/2/3 km, limit 90.
#   (plate, cam, index, pos, limit, +seconds, reading)
ROWS = [
    # NORMAL — steady 72 km/h: under the limit at every camera AND on average.
    ("NRM 1", 1, 1, 0, 1.0, 90.0, T0 + timedelta(seconds=0),   80.0),
    ("NRM 1", 1, 2, 1, 2.0, 90.0, T0 + timedelta(seconds=50),  82.0),
    ("NRM 1", 1, 3, 2, 3.0, 90.0, T0 + timedelta(seconds=100), 79.0),
    # SPEEDER — over the limit everywhere; ~144 km/h average.
    ("SPD 1", 1, 1, 0, 1.0, 90.0, T0 + timedelta(seconds=0),   130.0),
    ("SPD 1", 1, 2, 1, 2.0, 90.0, T0 + timedelta(seconds=25),  132.0),
    ("SPD 1", 1, 3, 2, 3.0, 90.0, T0 + timedelta(seconds=50),  128.0),
    # SNEAKY — UNDER the limit at every camera, but ~120 km/h average between them.
    ("SNK 1", 1, 1, 0, 1.0, 90.0, T0 + timedelta(seconds=0),   80.0),
    ("SNK 1", 1, 2, 1, 2.0, 90.0, T0 + timedelta(seconds=30),  82.0),
    ("SNK 1", 1, 3, 2, 3.0, 90.0, T0 + timedelta(seconds=60),  79.0),
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

    # Run the SAME fixture through both join strategies (the §5.2 comparison).
    avg_adj = [r for r in detect_average(events, "adjacent").collect()]
    avg_all = [r for r in detect_average(events, "all-pairs").collect()]
    adj_plates = sorted(r["car_plate"] for r in avg_adj)
    all_plates = sorted(r["car_plate"] for r in avg_all)
    final_adj = sorted((r["car_plate"], r["violation_type"])
                       for r in build_violations(events, "adjacent").collect())
    final_all = sorted((r["car_plate"], r["violation_type"])
                       for r in build_violations(events, "all-pairs").collect())

    print("\n--- raw detector output ---")
    print(f"instantaneous plates    : {sorted(inst)}")
    print(f"average rows (adjacent) : {len(avg_adj)} (plates {adj_plates})")
    print(f"average rows (all-pairs): {len(avg_all)} (plates {all_plates})")
    print("\n--- final de-duplicated violations (adjacent strategy) ---")
    for plate, vtype in final_adj:
        print(f"  {plate:6s} {vtype}")

    # Expected semantics (the whole point of the system). With the consecutive-segment join,
    # each violator yields 2 adjacent segments — (cam0,cam1) and (cam1,cam2) — while the
    # all-pairs baseline adds the redundant (cam0,cam2) bridge: 3 rows per violator. Final
    # dedup keys on car_plate alone: one flag per car per window, irrespective of type —
    # so a violator collapses to exactly ONE row under EITHER strategy.
    assert sorted(inst) == ["SPD 1"], f"instantaneous wrong: {sorted(inst)}"
    assert adj_plates == ["SNK 1", "SNK 1", "SPD 1", "SPD 1"], adj_plates              # k-1 = 2 each
    assert all_plates == ["SNK 1"] * 3 + ["SPD 1"] * 3, all_plates                     # k(k-1)/2 = 3 each

    final_plates = sorted(p for p, _ in final_adj)
    assert final_plates == ["SNK 1", "SPD 1"], final_adj  # NRM 1 clean; one row per violator
    # SPD 1 fires both types; car_plate-only dedup keeps exactly one (which type wins is not
    # significant — by design we only need to flag the car once per window).
    spd_types = [v for p, v in final_adj if p == "SPD 1"]
    assert len(spd_types) == 1 and spd_types[0] in ("AVERAGE", "INSTANTANEOUS"), final_adj

    # EQUIVALENCE: both strategies flag the same cars — the adjacent refinement loses no
    # enforcement power while emitting 4 join rows instead of 6 on this fixture.
    assert sorted(p for p, _ in final_all) == final_plates, (final_adj, final_all)

    print("\nPASS — normal=clean, speeder=flagged once, sneaky=flagged once (AVERAGE).")
    print(f"PASS — strategy equivalence: identical final flags {final_plates}; "
          f"join output {len(avg_adj)} rows (adjacent, k-1/vehicle) vs "
          f"{len(avg_all)} rows (all-pairs, k(k-1)/2/vehicle).")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

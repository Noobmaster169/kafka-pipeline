"""
The re-architected AWAS Spark Structured Streaming pipeline.

Reads the single partitioned `camera-events` topic, detects INSTANTANEOUS and AVERAGE
violations (the generalised self-join of proposal §4.2), de-duplicates per
(car_plate, violation_type), and sinks one document per violation to MongoDB while
republishing each to the `violations` Kafka topic for the live dashboard.

Run from src/:
    python -m pipeline.run            # start the streaming query
    python -m pipeline.run --reset    # clear checkpoint + drop violations, then start
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

# Make `common`/`pipeline` importable when run as a plain script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import config  # noqa: E402
from common.log import banner, get_logger  # noqa: E402
from common.mongo import ensure_indexes, get_db  # noqa: E402
from pipeline.detect import build_violations, read_events  # noqa: E402
from pipeline.sink import ViolationSink  # noqa: E402

log = get_logger("pipeline")

CHECKPOINT_PATH = Path(__file__).resolve().parents[2] / config.CHECKPOINT_DIR


def build_spark():
    """Create the SparkSession with the Kafka connector package wired in."""
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {config.SPARK_KAFKA_PACKAGE} pyspark-shell"
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master(config.SPARK_MASTER)
        .appName("AWAS-A3-pipeline")
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def reset_state() -> None:
    """Clear the streaming checkpoint and drop the violations collection."""
    if CHECKPOINT_PATH.exists():
        shutil.rmtree(CHECKPOINT_PATH)
        log.info("removed checkpoint %s", CHECKPOINT_PATH)
    get_db().drop_collection(config.COLL_VIOLATIONS)
    log.info("dropped collection %s", config.COLL_VIOLATIONS)


def show_banner(spark) -> None:
    banner("AWAS Spark Pipeline", [
        ("spark", spark.version),
        ("master", config.SPARK_MASTER),
        ("shuffle parts", config.SPARK_SHUFFLE_PARTITIONS),
        ("broker", config.KAFKA_BOOTSTRAP_SERVERS),
        ("in topic", config.KAFKA_TOPIC),
        ("out topic", config.KAFKA_VIOLATIONS_TOPIC),
        ("mongo", f"{config.MONGO_HOST}:{config.MONGO_PORT}/{config.MONGO_DB}"),
        ("watermark", config.WATERMARK_DURATION),
        ("join window", config.JOIN_WINDOW),
        ("dedup window", config.DEDUP_WINDOW),
        ("checkpoint", str(CHECKPOINT_PATH)),
    ])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AWAS Spark Structured Streaming pipeline.")
    p.add_argument("--reset", action="store_true",
                   help="Clear checkpoint and drop violations before starting.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.reset:
        reset_state()

    ensure_indexes()   # make sure the unique violations index exists before we write
    spark = build_spark()
    show_banner(spark)

    events = read_events(spark)
    violations = build_violations(events)

    CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)
    query = (
        violations.writeStream
        .outputMode("append")
        .foreachBatch(ViolationSink())
        .option("checkpointLocation", str(CHECKPOINT_PATH))
        .queryName("violations")
        .start()
    )
    log.info("streaming query started — waiting for events (Ctrl-C to stop)")
    # Heartbeat: the foreachBatch sink only sees *violations*, never the raw input, so we
    # read Spark's own per-micro-batch progress here to show traffic actually flowing. (The
    # Python StreamingQueryListener API only exists in Spark 3.4+, so on 3.3 we poll
    # recentProgress and de-dup by batchId.)
    seen: set[int] = set()
    total_in = 0
    last_beat = time.monotonic()
    try:
        while query.isActive:
            for p in query.recentProgress:
                bid = p["batchId"]
                if bid in seen:
                    continue
                seen.add(bid)
                n_in = int(p.get("numInputRows", 0) or 0)
                total_in += n_in
                if n_in:
                    log.info("batch %-4d events=%-4d total=%-6d  %.0f rows/s", bid, n_in,
                             total_in, p.get("processedRowsPerSecond", 0.0) or 0.0)
                    last_beat = time.monotonic()
            if time.monotonic() - last_beat > 30:
                log.info("idle — waiting for events (total processed=%d)", total_in)
                last_beat = time.monotonic()
            time.sleep(2)
    except KeyboardInterrupt:
        log.info("stopping ...")
        query.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

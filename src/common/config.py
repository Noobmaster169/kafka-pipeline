"""
Central configuration for every AWAS A3 service.

Every value is read from an environment variable with a sensible default, so the
*same* code runs unchanged whether a service is started inside the Docker network
(hosts `kafka` / `mongo`) or from the host with overridden env vars. Nothing is
hard-coded in the individual services — they all import from here.
"""

from __future__ import annotations

import os

# --------------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# The single, partitioned camera-event topic (A3's core change vs A2's 3 topics).
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "camera-events")

# How many partitions the topic is created with. This is THE scalability knob:
# parallelism for both ingest and the Spark join is bounded by this number.
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "6"))

# The pipeline re-publishes each detected violation here so the backend can show a
# live log WITHOUT relying on MongoDB change streams (which need a replica set).
KAFKA_VIOLATIONS_TOPIC = os.getenv("KAFKA_VIOLATIONS_TOPIC", "violations")

# --------------------------------------------------------------------------- #
# MongoDB
# --------------------------------------------------------------------------- #
# Default to localhost: the simulator/backend run on the host and reach the mongo
# container through its published port (-p 27017:27017). Override with MONGO_HOST=mongo
# when running inside the kafka-net Docker network.
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DB = os.getenv("MONGO_DB", "awas")

COLL_LANES = "lanes"
COLL_CAMERAS = "cameras"
COLL_CARS = "cars"
COLL_VIOLATIONS = "violations"

# --------------------------------------------------------------------------- #
# Streaming parameters (consumed by the Spark pipeline)
# --------------------------------------------------------------------------- #
# How late an event may arrive and still be accepted; also bounds join-state memory.
WATERMARK_DURATION = os.getenv("WATERMARK_DURATION", "10 minutes")

# Max gap between two camera crossings that can still be one journey (the join window).
JOIN_WINDOW = os.getenv("JOIN_WINDOW", "10 minutes")

# Which camera-pair predicate the AVERAGE self-join uses (REPORT.md §5.2/§8):
#   adjacent  — consecutive cameras only (|Δcamera_index| = 1): O(k) pairs per vehicle.
#               The A3 refinement and the production default.
#   all-pairs — every distinct camera pair: O(k²) pairs per vehicle. A2's semantics,
#               kept as the measured baseline for the join-strategy comparison.
JOIN_STRATEGY = os.getenv("JOIN_STRATEGY", "adjacent")

# Window over which repeated detections of the same vehicle collapse to one flag.
DEDUP_WINDOW = os.getenv("DEDUP_WINDOW", "10 minutes")

# Numeric form of DEDUP_WINDOW (whole minutes) — used to bucket a violation's start
# time into the per-car idempotency window that keys the unique violations index.
DEDUP_WINDOW_MINUTES = int(DEDUP_WINDOW.split()[0])

# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Post-shuffle partition count = parallelism of the stateful join/dedup operators.
# This is the Spark-side scalability knob the performance harness sweeps.
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")

# Spark-Kafka connector package (must match the Spark/Scala version of the image).
SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
)

# Streaming checkpoint directory (state + offsets survive restarts here).
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "dump/checkpoints/violations")

# --------------------------------------------------------------------------- #
# Domain defaults
# --------------------------------------------------------------------------- #
# Spacing applied when an admin appends a new camera to the end of a lane.
CAMERA_SPACING_KM = float(os.getenv("CAMERA_SPACING_KM", "1.0"))

# --------------------------------------------------------------------------- #
# Backend API (FastAPI) — Phase 4
# --------------------------------------------------------------------------- #
# Where uvicorn binds. 0.0.0.0 so the dashboard can reach it whether the API runs
# on the host or inside a container.
BACKEND_HOST = os.getenv("BACKEND_HOST", "0.0.0.0")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", "8000"))

# Browser origins allowed by CORS. The React dashboard is served by the Vite dev
# server (5173) in development; override with a comma-separated list in production.
CORS_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173"
    ).split(",")
    if o.strip()
]

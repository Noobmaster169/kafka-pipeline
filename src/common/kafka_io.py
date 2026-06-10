"""
Thin Kafka producer/consumer factories shared by the simulator, pipeline, and backend.

Uses `kafka3` (the same client A2 used). JSON values; the car plate is the message
key so all of a vehicle's events land on one partition, in order — the property the
stream-stream join depends on.
"""

from __future__ import annotations

import json
import logging

from common import config
from common.log import get_logger

log = get_logger("kafkaio")

# kafka-python's own loggers (kafka.conn, kafka.consumer, ...) are very chatty at
# INFO — per-connection lifecycle and partition-assignment lines that drown our
# clean output. Give that hierarchy our handler but only at WARNING+, so genuine
# broker problems still surface (in our format) while the routine chatter is muted.
get_logger("kafka").setLevel(logging.WARNING)

# The kafka client is imported lazily inside the factories so non-Kafka services
# (and tests) can import this module without the client installed.


def make_producer(bootstrap: str | None = None):
    """A JSON-serialising producer that keys messages by string (car_plate)."""
    from kafka import KafkaProducer

    servers = bootstrap or config.KAFKA_BOOTSTRAP_SERVERS
    log.debug("creating producer -> %s", servers)
    producer = KafkaProducer(
        bootstrap_servers=[servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        api_version=(0, 10, 0),
        retries=3,
        linger_ms=20,
        acks="all",
    )
    log.info("Kafka producer ready (%s)", servers)
    return producer


def make_consumer(
    topic: str | None = None,
    *,
    group_id: str | None = None,
    bootstrap: str | None = None,
    auto_offset_reset: str = "latest",
    **kwargs,
):
    """A JSON-deserialising consumer subscribed to one topic."""
    from kafka import KafkaConsumer

    sub = topic or config.KAFKA_TOPIC
    servers = bootstrap or config.KAFKA_BOOTSTRAP_SERVERS
    log.debug("creating consumer -> topic=%s group=%s offset=%s", sub, group_id, auto_offset_reset)
    consumer = KafkaConsumer(
        sub,
        bootstrap_servers=[servers],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        api_version=(0, 10, 0),
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        **kwargs,
    )
    log.info("Kafka consumer ready (topic=%s group=%s)", sub, group_id)
    return consumer

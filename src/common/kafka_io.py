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


def ensure_topics(bootstrap: str | None = None) -> None:
    """
    Best-effort creation of the partitioned `camera-events` topic at startup.

    Goal: make config.KAFKA_TOPIC_PARTITIONS real — without an explicit create the
    broker lazily auto-creates the topic at its default (num.partitions, usually 1)
    on first produce, capping ingest parallelism at 1 (proposal §4.1 / D1).

    IMPORTANT — this is best-effort and never fatal. The unit's `fit3182/kafka` image
    is an old broker (Kafka 0.10.x) that predates the CreateTopics admin API (KIP-4),
    so the Python admin client cannot create topics against it and raises
    IncompatibleBrokerVersion. We catch that (and any other broker/connection error),
    log a warning, and continue: the broker still auto-creates the topic on first
    produce, and the partition count can be set explicitly with the broker CLI
    (`deployment/scripts/stack.sh topics`, which runs kafka-topics.sh via ZooKeeper).

    On a modern broker (>= 0.10.1) the admin create succeeds and sets the partition
    count directly. Either way startup is never blocked.

    Caveat: Kafka cannot shrink partitions, and an existing topic is left untouched
    (TopicAlreadyExistsError is swallowed). The count takes effect only on a *fresh*
    topic; we deliberately do NOT grow an existing one (adding partitions remaps
    hash(key) % n and would break the per-vehicle ordering the join relies on).
    """
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    servers = bootstrap or config.KAFKA_BOOTSTRAP_SERVERS
    admin = None
    try:
        admin = KafkaAdminClient(bootstrap_servers=[servers], api_version=(0, 10, 0))
        topic = NewTopic(
            name=config.KAFKA_TOPIC,
            num_partitions=config.KAFKA_TOPIC_PARTITIONS,
            replication_factor=1,
        )
        admin.create_topics([topic])
        log.info("topic %s created (%d partitions)",
                 config.KAFKA_TOPIC, config.KAFKA_TOPIC_PARTITIONS)
    except TopicAlreadyExistsError:
        log.info("topic %s already exists — left as-is (not repartitioned)",
                 config.KAFKA_TOPIC)
    except Exception as exc:  # old broker (no CreateTopics API), unreachable, etc.
        log.warning(
            "ensure_topics: skipping programmatic create for %s (%s: %s). "
            "Broker will auto-create it; set partitions with 'stack.sh topics'.",
            config.KAFKA_TOPIC, type(exc).__name__, exc,
        )
    finally:
        if admin is not None:
            admin.close()


def topic_partition_count(topic: str | None = None, bootstrap: str | None = None) -> int:
    """
    Read-only, best-effort: number of partitions the broker reports for `topic`.

    Returns 0 if the topic does not exist yet or the broker can't be reached. Never
    raises — it is called from service startup banners, which must not be able to crash.
    """
    from kafka import KafkaConsumer

    name = topic or config.KAFKA_TOPIC
    servers = bootstrap or config.KAFKA_BOOTSTRAP_SERVERS
    consumer = None
    try:
        consumer = KafkaConsumer(bootstrap_servers=[servers], api_version=(0, 10, 0))
        parts = consumer.partitions_for_topic(name)
        return len(parts) if parts else 0
    except Exception:
        return 0
    finally:
        if consumer is not None:
            consumer.close()


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

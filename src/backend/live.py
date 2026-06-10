"""
The live-feed hub: bridges the two Kafka topics to connected WebSockets.

The dashboard's real-time views are driven entirely by Kafka (proposal §4.1 / §4.4),
not by MongoDB change streams (the `fit3182/mongo` image is standalone and has none):

  - `camera-events`  → every crossing, routed to the matching lane's subscribers, so
                       the lane schematic can animate cars as moving dots.
  - `violations`     → each newly detected violation, fanned out to the live log.

`kafka-python` is a blocking client, so each topic is drained on its own daemon
thread; messages are handed back to the asyncio event loop with
`run_coroutine_threadsafe`. Consumer creation is retried inside the thread, so the
backend starts (and serves REST) even when no broker is up yet — the feed simply
reconnects when Kafka appears.
"""

from __future__ import annotations

import asyncio
import threading
import time

from fastapi import WebSocket

from common import config
from common.kafka_io import make_consumer
from common.log import get_logger

log = get_logger("backend.live")

# How long to wait before retrying a broker that wasn't reachable.
_RECONNECT_SECONDS = 5.0


class Hub:
    """Registry of live WebSocket subscribers plus the Kafka consumer threads."""

    def __init__(self) -> None:
        self._lane_subs: dict[int, set[WebSocket]] = {}
        self._violation_subs: set[WebSocket] = set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._threads: list[threading.Thread] = []
        self._consumers: list = []          # KafkaConsumers, kept so stop() can close them
        self._stop = threading.Event()

    # ----------------------------------------------------------------- #
    # Subscription bookkeeping (called from the WebSocket route handlers)
    # ----------------------------------------------------------------- #
    async def connect_lane(self, ws: WebSocket, lane_id: int) -> None:
        await ws.accept()
        self._lane_subs.setdefault(lane_id, set()).add(ws)

    def disconnect_lane(self, ws: WebSocket, lane_id: int) -> None:
        self._lane_subs.get(lane_id, set()).discard(ws)

    async def connect_violations(self, ws: WebSocket) -> None:
        await ws.accept()
        self._violation_subs.add(ws)

    def disconnect_violations(self, ws: WebSocket) -> None:
        self._violation_subs.discard(ws)

    # ----------------------------------------------------------------- #
    # Fan-out (awaited on the event loop)
    # ----------------------------------------------------------------- #
    async def _broadcast(self, subscribers: set[WebSocket], message: dict) -> None:
        dead: list[WebSocket] = []
        for ws in subscribers:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)   # client gone mid-send; prune it
        for ws in dead:
            subscribers.discard(ws)

    async def broadcast_lane(self, lane_id: int, message: dict) -> None:
        subs = self._lane_subs.get(lane_id)
        if subs:
            await self._broadcast(subs, message)

    async def broadcast_violation(self, message: dict) -> None:
        if self._violation_subs:
            await self._broadcast(self._violation_subs, message)

    # ----------------------------------------------------------------- #
    # Lifecycle (called from the app lifespan)
    # ----------------------------------------------------------------- #
    def start(self) -> None:
        """Capture the running loop and launch the two consumer threads."""
        self._loop = asyncio.get_running_loop()
        self._spawn("camera-events", config.KAFKA_TOPIC, self._on_camera_event)
        self._spawn("violations", config.KAFKA_VIOLATIONS_TOPIC, self._on_violation)

    def stop(self) -> None:
        """Signal the threads to finish and close their consumers."""
        self._stop.set()
        for consumer in self._consumers:
            try:
                consumer.close()
            except Exception:
                pass
        for thread in self._threads:
            thread.join(timeout=2.0)

    def _spawn(self, label: str, topic: str, handler) -> None:
        thread = threading.Thread(
            target=self._consume_loop, args=(label, topic, handler),
            name=f"kafka-{label}", daemon=True,
        )
        thread.start()
        self._threads.append(thread)

    # ----------------------------------------------------------------- #
    # Per-topic consumer loop (runs on its own daemon thread)
    # ----------------------------------------------------------------- #
    def _consume_loop(self, label: str, topic: str, handler) -> None:
        """Connect (retrying), then poll the topic and hand each message to the loop."""
        warned_down = False
        while not self._stop.is_set():
            try:
                consumer = make_consumer(
                    topic,
                    group_id=None,                  # broadcast feed: every backend sees all
                    auto_offset_reset="latest",     # only live traffic, not history
                    consumer_timeout_ms=1000,       # so poll() returns and we can re-check stop
                )
            except Exception as exc:
                if not warned_down:
                    log.warning("live feed '%s' degraded — Kafka unreachable (%s); retrying",
                                label, type(exc).__name__)
                    warned_down = True
                time.sleep(_RECONNECT_SECONDS)
                continue

            self._consumers.append(consumer)
            log.info("live feed '%s' connected -> topic=%s", label, topic)
            warned_down = False
            try:
                while not self._stop.is_set():
                    records = consumer.poll(timeout_ms=1000)
                    for batch in records.values():
                        for record in batch:
                            self._dispatch(handler, record.value)
            except Exception as exc:
                log.warning("live feed '%s' dropped (%s); reconnecting",
                            label, type(exc).__name__)
            finally:
                try:
                    consumer.close()
                except Exception:
                    pass
                if consumer in self._consumers:
                    self._consumers.remove(consumer)

    def _dispatch(self, handler, value: dict) -> None:
        """Schedule the (async) handler on the event loop from this worker thread."""
        if self._loop is None:
            return
        asyncio.run_coroutine_threadsafe(handler(value), self._loop)

    async def _on_camera_event(self, event: dict) -> None:
        lane_id = event.get("lane_id")
        if lane_id is not None:
            await self.broadcast_lane(int(lane_id), event)

    async def _on_violation(self, violation: dict) -> None:
        await self.broadcast_violation(violation)


# One hub per process, shared by the app and the WebSocket routes.
hub = Hub()

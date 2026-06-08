"""
Trip generation — the 'logic' core of the simulator.

Given a car, the cameras on a lane (ordered along the road), and a behaviour, produce
the sequence of camera-detection events that car would generate. The timestamps and
spot-speed readings are constructed so the behaviour is exactly what the pipeline will
classify:

  normal  — cruises below the limit everywhere; no violation.
  speeder — drives over the limit; high spot readings (INSTANTANEOUS) and high average
            (AVERAGE).
  sneaky  — brakes to UNDER the limit at every camera (no INSTANTANEOUS), but covers the
            ground between cameras fast enough that the AVERAGE speed exceeds the limit.
            The case spot-speed enforcement misses and average-speed enforcement catches.

`generate_trip` is pure (no Kafka/Mongo), so it is unit-testable in isolation.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from common.schema import build_event

BEHAVIORS = ("normal", "speeder", "sneaky")


def pick_behavior(rng: random.Random, ratios: dict[str, float]) -> str:
    """Sample a behaviour from a {name: probability} mix (assumed to sum to ~1)."""
    r = rng.random()
    cumulative = 0.0
    for name in BEHAVIORS:
        cumulative += ratios.get(name, 0.0)
        if r <= cumulative:
            return name
    return BEHAVIORS[0]


def generate_trip(
    car_plate: str,
    lane_cameras: list[dict],
    behavior: str,
    start_time: datetime,
    rng: random.Random,
) -> list[tuple[float, dict]]:
    """
    Produce a car's crossings down one lane.

    Returns a list of `(offset_seconds, event_dict)` ordered by offset, where
    `offset_seconds` is the time since the trip start at which the crossing occurs.
    The caller decides whether to honour that timing (live mode) or ignore it
    (load-test mode). Event timestamps are already set to `start_time + offset`.
    """
    cams = sorted(lane_cameras, key=lambda c: c["position_km"])
    limits = [float(c["speed_limit"]) for c in cams]
    min_limit, max_limit = min(limits), max(limits)

    # `between` = the speed used to time travel between cameras (drives the AVERAGE).
    # `spot(limit)` = the instantaneous reading recorded AT a camera.
    if behavior == "normal":
        between = rng.uniform(0.70, 0.95) * min_limit
        def spot(limit: float) -> float:
            return round(min(between + rng.uniform(-3.0, 3.0), limit - 1.0), 1)
    elif behavior == "speeder":
        between = rng.uniform(1.15, 1.45) * max_limit
        def spot(limit: float) -> float:
            return round(between + rng.uniform(-4.0, 6.0), 1)          # over the limit
    elif behavior == "sneaky":
        between = rng.uniform(1.20, 1.50) * max_limit
        def spot(limit: float) -> float:
            return round(rng.uniform(0.85, 0.97) * limit, 1)          # under each limit
    else:
        raise ValueError(f"unknown behaviour: {behavior!r}")

    events: list[tuple[float, dict]] = []
    offset = 0.0
    prev = None
    for cam in cams:
        if prev is not None:
            distance_km = float(cam["position_km"]) - float(prev["position_km"])
            offset += distance_km / between * 3600.0   # seconds to cover the segment
        event = build_event(
            car_plate=car_plate,
            lane_id=int(cam["lane_id"]),
            camera_id=int(cam["camera_id"]),
            position_km=float(cam["position_km"]),
            speed_limit=float(cam["speed_limit"]),
            timestamp=start_time + timedelta(seconds=offset),
            speed_reading=spot(float(cam["speed_limit"])),
        )
        events.append((offset, event))
        prev = cam
    return events


def summarize_trip(events: list[tuple[float, dict]]) -> dict:
    """
    Describe what a generated trip should trigger — used for informative logging.

    Returns the peak spot reading, the peak segment average speed, and whether the
    pipeline is expected to raise an INSTANTANEOUS and/or an AVERAGE violation.
    """
    max_spot = max((e["speed_reading"] for _, e in events), default=0.0)
    expect_instantaneous = any(e["speed_reading"] > e["speed_limit"] for _, e in events)

    max_avg = 0.0
    expect_average = False
    for i in range(len(events)):
        off_i, ev_i = events[i]
        for j in range(i + 1, len(events)):
            off_j, ev_j = events[j]
            dt = off_j - off_i
            if dt <= 0:
                continue
            avg = (ev_j["position_km"] - ev_i["position_km"]) * 3600.0 / dt
            max_avg = max(max_avg, avg)
            if avg > ev_j["speed_limit"]:   # end-camera limit governs the segment
                expect_average = True

    return {
        "n_events": len(events),
        "max_spot": round(max_spot, 1),
        "max_avg": round(max_avg, 1),
        "expect_instantaneous": expect_instantaneous,
        "expect_average": expect_average,
    }

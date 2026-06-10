"""
Request bodies for the write endpoints.

Only the two `POST`s need a typed body; everything else is a read with query
parameters. Kept deliberately small — these mirror the documents in `common.mongo`.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class CameraCreate(BaseModel):
    """Append a camera to a lane. position_km and camera_id are assigned server-side
    (auto-append), so the client only chooses the lane and, optionally, the limit."""

    lane_id: int
    # Omit to inherit the limit already enforced on that lane's cameras.
    speed_limit: float | None = Field(default=None, gt=0)


class CarCreate(BaseModel):
    """Register a new vehicle in the car pool."""

    car_plate: str
    owner_name: str
    owner_addr: str
    vehicle_type: str
    registration_date: str

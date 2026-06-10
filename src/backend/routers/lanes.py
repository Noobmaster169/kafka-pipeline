"""Lane endpoints — the dashboard overview and per-lane detail."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend import db

router = APIRouter(prefix="/lanes", tags=["lanes"])


@router.get("")
def list_lanes() -> list[dict]:
    """All lanes with camera count and violation tallies."""
    return db.list_lanes_with_summary()


@router.get("/{lane_id}")
def get_lane(lane_id: int) -> dict:
    """One lane with its ordered cameras and violation summary."""
    lane = db.get_lane(lane_id)
    if lane is None:
        raise HTTPException(status_code=404, detail=f"lane {lane_id} not found")
    return lane

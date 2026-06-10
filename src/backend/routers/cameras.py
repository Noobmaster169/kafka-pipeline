"""Camera endpoints — list, and the runtime auto-append that drives the hot-add demo."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend import db
from backend.models import CameraCreate

router = APIRouter(prefix="/cameras", tags=["cameras"])


@router.get("")
def list_cameras(lane_id: int | None = None) -> list[dict]:
    """All cameras, or one lane's, ordered along the road."""
    return db.list_cameras(lane_id)


@router.post("", status_code=201)
def create_camera(body: CameraCreate) -> dict:
    """Append a camera to the end of a lane (position + id assigned server-side)."""
    try:
        return db.append_camera(body.lane_id, body.speed_limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    
@router.delete("/last")
def remove_last_camera(lane_id: int) -> dict:
    """Remove the camera at the end of a lane (the inverse of the auto-append)."""
    try:
        camera = db.remove_last_camera(lane_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    if camera is None:
        raise HTTPException(status_code=404, detail=f"lane {lane_id} does not exist")
    return {"removed": camera}

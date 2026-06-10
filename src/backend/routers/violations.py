"""Violation endpoints — filtered list, single lookup, and a streamed CSV export."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from backend import db

router = APIRouter(prefix="/violations", tags=["violations"])


@router.get("")
def list_violations(
    lane_id: int | None = None,
    violation_type: str | None = Query(None, pattern="^(INSTANTANEOUS|AVERAGE)$"),
    car_plate: str | None = None,
    date: str | None = Query(None, description="ISO date, e.g. 2026-06-09"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    """Filtered, paginated violations (newest detection first) with a total count."""
    return db.query_violations(
        lane_id=lane_id, violation_type=violation_type, car_plate=car_plate,
        date=date, skip=skip, limit=limit,
    )


@router.get("/export.csv")
def export_violations_csv(
    lane_id: int | None = None,
    violation_type: str | None = Query(None, pattern="^(INSTANTANEOUS|AVERAGE)$"),
    car_plate: str | None = None,
    date: str | None = None,
) -> StreamingResponse:
    """Stream the filtered violations as a downloadable CSV."""
    rows = db.iter_violations_csv(
        lane_id=lane_id, violation_type=violation_type, car_plate=car_plate, date=date,
    )
    return StreamingResponse(
        rows,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=violations.csv"},
    )


@router.get("/{violation_id}")
def get_violation(violation_id: str) -> dict:
    """One violation by its id."""
    violation = db.get_violation(violation_id)
    if violation is None:
        raise HTTPException(status_code=404, detail="violation not found")
    return violation

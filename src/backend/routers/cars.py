"""Car endpoints — search the pool, view a car with its violations, register a new car."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pymongo.errors import DuplicateKeyError

from backend import db
from backend.models import CarCreate

router = APIRouter(prefix="/cars", tags=["cars"])


@router.get("")
def list_cars(
    plate: str | None = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    """Paginated cars, optionally filtered by a case-insensitive plate prefix."""
    return db.search_cars(plate, skip, limit)


@router.get("/{plate}")
def get_car(plate: str) -> dict:
    """A car with its violation history (newest first)."""
    car = db.get_car_with_violations(plate)
    if car is None:
        raise HTTPException(status_code=404, detail=f"car {plate} not found")
    return car


@router.post("", status_code=201)
def create_car(body: CarCreate) -> dict:
    """Register a new vehicle. 409 if the plate already exists."""
    try:
        return db.create_car(body.model_dump())
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail=f"car {body.car_plate} already exists")

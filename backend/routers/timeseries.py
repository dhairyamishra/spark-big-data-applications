"""Time series demand endpoints."""

from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter()


def _get_store():
    from main import store
    return store


@router.get("")
async def get_demand_timeseries(
    zone_id: Optional[int] = Query(None),
    borough: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(1000, ge=1, le=50000),
):
    """
    Return daily demand time series.
    Filter by zone_id, borough, and/or date range.
    """
    s = _get_store()
    data = s.get("daily_demand", [])

    if zone_id is not None:
        data = [r for r in data if r.get("pu_location_id") == zone_id]
    if borough:
        borough_lower = borough.lower()
        data = [r for r in data if (r.get("pu_borough") or "").lower() == borough_lower]
    if start_date:
        data = [r for r in data if (r.get("pickup_date") or "") >= start_date]
    if end_date:
        data = [r for r in data if (r.get("pickup_date") or "") <= end_date]

    return data[:limit]


@router.get("/borough")
async def get_borough_timeseries(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Return daily demand aggregated by borough."""
    s = _get_store()
    data = s.get("borough_daily", [])

    if start_date:
        data = [r for r in data if (r.get("pickup_date") or "") >= start_date]
    if end_date:
        data = [r for r in data if (r.get("pickup_date") or "") <= end_date]

    return data

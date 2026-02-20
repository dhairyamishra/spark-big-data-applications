"""Global statistics and overview endpoints."""

from fastapi import APIRouter

router = APIRouter()


def _get_store():
    from main import store
    return store


@router.get("/overview")
async def get_overview():
    """Return high-level platform statistics."""
    s = _get_store()
    return s.get("overview", {})


@router.get("/borough_daily")
async def get_borough_daily():
    """Return daily stats per borough."""
    s = _get_store()
    return s.get("borough_daily", [])


@router.get("/temporal_patterns")
async def get_temporal_patterns(zone_id: int = None):
    """
    Return hour × day_of_week heatmap data.
    Optionally filtered to a single zone.
    """
    s = _get_store()
    data = s.get("temporal_patterns", [])

    if zone_id is not None:
        data = [r for r in data if r.get("pu_location_id") == zone_id]

    return data

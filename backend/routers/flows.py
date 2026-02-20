"""Origin-destination flow endpoints."""

from fastapi import APIRouter, Query

router = APIRouter()


def _get_store():
    from main import store
    return store


@router.get("")
async def get_od_flows(
    limit: int = Query(200, ge=1, le=500),
    min_trips: int = Query(0, ge=0),
):
    """
    Return top origin-destination flows for the arc map.
    Each flow has pickup/dropoff zone, trip count, avg fare.
    """
    s = _get_store()
    data = s.get("od_flows", [])

    if min_trips > 0:
        data = [r for r in data if (r.get("trip_count") or 0) >= min_trips]

    data.sort(key=lambda x: x.get("trip_count", 0), reverse=True)
    return data[:limit]

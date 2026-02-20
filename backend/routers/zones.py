"""Zone geometry and statistics endpoints."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

router = APIRouter()


def _get_store():
    from main import store
    return store


@router.get("")
async def get_zones(borough: Optional[str] = Query(None)):
    """Return zone GeoJSON with stats. Optionally filter by borough."""
    s = _get_store()
    geojson = s.get("taxi_zones_geojson", {})
    if not geojson:
        return {"type": "FeatureCollection", "features": []}

    if borough:
        borough_lower = borough.lower()
        if isinstance(geojson, dict) and "features" in geojson:
            filtered = {
                "type": "FeatureCollection",
                "features": [
                    f for f in geojson["features"]
                    if f.get("properties", {}).get("Borough", "").lower() == borough_lower
                ],
            }
            return filtered

    return geojson


@router.get("/stats")
async def get_all_zone_stats(
    borough: Optional[str] = Query(None),
    sort_by: str = Query("total_trips", description="Sort field"),
    limit: int = Query(50, ge=1, le=265),
):
    """Return zone-level statistics, optionally filtered and sorted."""
    s = _get_store()
    data = list(s.get("zone_stats", []))

    if borough:
        borough_lower = borough.lower()
        data = [z for z in data if (z.get("borough") or "").lower() == borough_lower]

    data.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=True)
    return data[:limit]


@router.get("/{zone_id}")
async def get_zone_detail(zone_id: int):
    """Return detailed stats for a single zone."""
    s = _get_store()
    zone = s.get("zone_stats_by_id", {}).get(zone_id)
    if not zone:
        raise HTTPException(status_code=404, detail=f"Zone {zone_id} not found")

    cluster = s.get("clusters_by_id", {}).get(zone_id, {})
    zone_anomalies = [
        a for a in s.get("anomalies", [])
        if a.get("pu_location_id") == zone_id
    ][:20]

    return {
        "stats": zone,
        "cluster": cluster,
        "recent_anomalies": zone_anomalies,
    }

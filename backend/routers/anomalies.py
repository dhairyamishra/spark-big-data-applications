"""Anomaly detection endpoints."""

from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter()


def _get_store():
    from main import store
    return store


@router.get("")
async def get_anomalies(
    zone_id: Optional[int] = Query(None),
    severity: Optional[str] = Query(None, description="critical, high, medium"),
    direction: Optional[str] = Query(None, description="surge or drop"),
    limit: int = Query(200, ge=1, le=2000),
):
    """
    Return detected anomalies, sorted by z-score magnitude.
    """
    s = _get_store()
    data = s.get("anomalies", [])

    if zone_id is not None:
        data = [r for r in data if r.get("pu_location_id") == zone_id]
    if severity:
        data = [r for r in data if r.get("severity") == severity]
    if direction:
        data = [r for r in data if r.get("direction") == direction]

    data.sort(key=lambda x: abs(x.get("z_score", 0)), reverse=True)
    return data[:limit]


@router.get("/summary")
async def get_anomaly_summary():
    """Aggregate anomaly counts by severity and direction."""
    s = _get_store()
    data = s.get("anomalies", [])

    summary = {
        "total": len(data),
        "by_severity": {},
        "by_direction": {"surge": 0, "drop": 0},
        "top_zones": {},
    }

    from collections import Counter
    severity_counts = Counter(r.get("severity") for r in data)
    summary["by_severity"] = dict(severity_counts)

    direction_counts = Counter(r.get("direction") for r in data)
    summary["by_direction"] = dict(direction_counts)

    zone_counts = Counter(r.get("pu_location_id") for r in data)
    summary["top_zones"] = dict(zone_counts.most_common(10))

    return summary

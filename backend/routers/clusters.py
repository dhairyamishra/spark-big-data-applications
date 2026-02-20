"""Zone clustering endpoints."""

from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter()


def _get_store():
    from main import store
    return store


@router.get("")
async def get_clusters(cluster_id: Optional[int] = Query(None)):
    """Return zone cluster assignments."""
    s = _get_store()
    data = s.get("clusters", [])

    if cluster_id is not None:
        data = [r for r in data if r.get("cluster_id") == cluster_id]

    return data


@router.get("/profiles")
async def get_cluster_profiles():
    """Return aggregate profile for each cluster."""
    s = _get_store()
    data = s.get("clusters", [])

    if not data:
        return []

    from collections import defaultdict
    groups = defaultdict(list)
    for r in data:
        groups[r.get("cluster_id")].append(r)

    profiles = []
    for cid, members in sorted(groups.items()):
        n = len(members)
        profiles.append({
            "cluster_id": cid,
            "zone_count": n,
            "avg_trips": sum(m.get("total_trips", 0) for m in members) / n,
            "avg_fare": sum(m.get("avg_fare", 0) or 0 for m in members) / n,
            "avg_distance": sum(m.get("avg_distance", 0) or 0 for m in members) / n,
            "avg_tip_pct": sum(m.get("avg_tip_pct", 0) or 0 for m in members) / n,
            "boroughs": list(set(m.get("borough") for m in members if m.get("borough"))),
            "zones": [m.get("zone_name") for m in members if m.get("zone_name")],
        })

    return profiles

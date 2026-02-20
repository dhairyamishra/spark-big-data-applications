"""ML demand prediction endpoints."""

from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter()


def _get_store():
    from main import store
    return store


@router.get("")
async def get_predictions(
    zone_id: Optional[int] = Query(None),
    limit: int = Query(500, ge=1, le=10000),
):
    """
    Return demand predictions (actual vs predicted).
    From GBTRegressor model trained on 2023, tested on 2024.
    """
    s = _get_store()
    data = s.get("predictions", [])

    if zone_id is not None:
        data = [r for r in data if r.get("pu_location_id") == zone_id]

    data.sort(key=lambda x: x.get("datetime_hour", ""))
    return data[:limit]


@router.get("/accuracy")
async def get_prediction_accuracy():
    """Return aggregated accuracy metrics per zone."""
    s = _get_store()
    data = s.get("predictions", [])

    if not data:
        return {"zones": [], "overall": {}}

    from collections import defaultdict
    zone_errors = defaultdict(list)
    for r in data:
        zid = r.get("pu_location_id")
        actual = r.get("trip_count", 0) or 0
        predicted = r.get("predicted_demand", 0) or 0
        zone_errors[zid].append((actual, predicted))

    zone_metrics = []
    all_errors = []
    for zid, pairs in zone_errors.items():
        errors = [(a - p) ** 2 for a, p in pairs]
        mae_vals = [abs(a - p) for a, p in pairs]
        rmse = (sum(errors) / len(errors)) ** 0.5
        mae = sum(mae_vals) / len(mae_vals)
        zone_metrics.append({
            "zone_id": zid,
            "rmse": round(rmse, 2),
            "mae": round(mae, 2),
            "samples": len(pairs),
        })
        all_errors.extend(errors)

    overall_rmse = (sum(all_errors) / len(all_errors)) ** 0.5 if all_errors else 0

    return {
        "zones": sorted(zone_metrics, key=lambda x: x["rmse"]),
        "overall": {
            "rmse": round(overall_rmse, 2),
            "total_samples": len(data),
        },
    }

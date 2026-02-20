"""
FastAPI backend for the NYC Urban Mobility Intelligence Platform.

All data is pre-computed by the Spark pipeline and loaded into memory
at startup from JSON files. Every endpoint is a dictionary lookup — 
sub-millisecond latency, zero runtime DB queries.
"""

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from routers import zones, timeseries, flows, predictions, anomalies, clusters, stats

# ---------------------------------------------------------------------------
# Data store — loaded once at startup, shared across all routers
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent / "data"

store: dict = {}


def load_json(name: str) -> list | dict:
    """Load a JSON artifact from the data directory."""
    path = DATA_DIR / f"{name}.json"
    if not path.exists():
        print(f"  [WARN] {path} not found — endpoint will return empty data")
        return []
    with open(path, "r") as f:
        data = json.load(f)
    size_kb = path.stat().st_size / 1024
    count = len(data) if isinstance(data, list) else "dict"
    print(f"  ✓ {name}: {count} records ({size_kb:.0f} KB)")
    return data


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load all JSON data into memory at startup."""
    print("Loading data artifacts...")
    store["zone_stats"] = load_json("zone_stats")
    store["daily_demand"] = load_json("daily_demand")
    store["od_flows"] = load_json("od_flows")
    store["borough_daily"] = load_json("borough_daily")
    store["temporal_patterns"] = load_json("temporal_patterns")
    store["predictions"] = load_json("predictions")
    store["clusters"] = load_json("clusters")
    store["anomalies"] = load_json("anomalies")
    store["overview"] = load_json("overview")
    store["taxi_zones_geojson"] = load_json("taxi_zones")

    # Build lookup indices for fast access
    store["zone_stats_by_id"] = {
        r["zone_id"]: r for r in store["zone_stats"]
    } if store["zone_stats"] else {}

    store["clusters_by_id"] = {
        r["zone_id"]: r for r in store["clusters"]
    } if store["clusters"] else {}

    print(f"✓ All data loaded. {len(store)} stores ready.\n")
    yield
    store.clear()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="NYC Urban Mobility Intelligence Platform",
    version="1.0.0",
    description="Spark-powered analytics for 200M+ NYC taxi trips",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(zones.router, prefix="/api/zones", tags=["Zones"])
app.include_router(timeseries.router, prefix="/api/timeseries", tags=["Time Series"])
app.include_router(flows.router, prefix="/api/flows", tags=["OD Flows"])
app.include_router(predictions.router, prefix="/api/predictions", tags=["Predictions"])
app.include_router(anomalies.router, prefix="/api/anomalies", tags=["Anomalies"])
app.include_router(clusters.router, prefix="/api/clusters", tags=["Clusters"])
app.include_router(stats.router, prefix="/api/stats", tags=["Statistics"])


@app.get("/api/health")
async def health():
    return {"status": "ok", "stores_loaded": len(store)}

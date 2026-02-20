#!/usr/bin/env python3
"""
Generate realistic demo JSON data so the web app works out of the box
without running the full Spark pipeline.

This creates ~5MB of sample data mimicking the pipeline's gold-layer output.

Usage:
    python generate_demo_data.py
"""

import json
import math
import os
import random
from datetime import datetime, timedelta

random.seed(42)
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "backend", "data")

# ---------------------------------------------------------------------------
# NYC Taxi Zone reference data (subset of real zones)
# ---------------------------------------------------------------------------
ZONES = [
    (1, "Newark Airport", "EWR"),
    (4, "Alphabet City", "Manhattan"),
    (7, "Astoria", "Queens"),
    (12, "Battery Park", "Manhattan"),
    (13, "Battery Park City", "Manhattan"),
    (24, "Borough Park", "Brooklyn"),
    (36, "Central Harlem", "Manhattan"),
    (37, "Central Harlem North", "Manhattan"),
    (42, "Central Park", "Manhattan"),
    (43, "Chelsea", "Manhattan"),
    (45, "Chinatown", "Manhattan"),
    (48, "Clinton East", "Manhattan"),
    (50, "Clinton West", "Manhattan"),
    (68, "East Chelsea", "Manhattan"),
    (74, "East Harlem North", "Manhattan"),
    (75, "East Harlem South", "Manhattan"),
    (79, "East Village", "Manhattan"),
    (87, "Financial District North", "Manhattan"),
    (88, "Financial District South", "Manhattan"),
    (90, "Flatiron", "Manhattan"),
    (100, "Garment District", "Manhattan"),
    (107, "Gramercy", "Manhattan"),
    (113, "Greenwich Village North", "Manhattan"),
    (114, "Greenwich Village South", "Manhattan"),
    (125, "Hudson Sq", "Manhattan"),
    (127, "Hudson Yards", "Manhattan"),
    (128, "Inwood", "Manhattan"),
    (130, "Jackson Heights", "Queens"),
    (132, "JFK Airport", "Queens"),
    (137, "Kips Bay", "Manhattan"),
    (140, "LaGuardia Airport", "Queens"),
    (141, "Lenox Hill East", "Manhattan"),
    (142, "Lenox Hill West", "Manhattan"),
    (143, "Lincoln Square East", "Manhattan"),
    (144, "Lincoln Square West", "Manhattan"),
    (148, "Lower East Side", "Manhattan"),
    (151, "Manhattan Valley", "Manhattan"),
    (152, "Marble Hill", "Manhattan"),
    (153, "Mariners Harbor", "Staten Island"),
    (158, "Meatpacking/West Village W", "Manhattan"),
    (161, "Midtown Center", "Manhattan"),
    (162, "Midtown East", "Manhattan"),
    (163, "Midtown North", "Manhattan"),
    (164, "Midtown South", "Manhattan"),
    (166, "Morningside Heights", "Manhattan"),
    (170, "Murray Hill", "Manhattan"),
    (186, "Penn Station/Madison Sq West", "Manhattan"),
    (194, "Randalls Island", "Manhattan"),
    (202, "Roosevelt Island", "Manhattan"),
    (209, "Seaport", "Manhattan"),
    (211, "SoHo", "Manhattan"),
    (224, "Stuy Town/PCV", "Manhattan"),
    (229, "Sutton Place/Turtle Bay North", "Manhattan"),
    (230, "Sutton Place/Turtle Bay South", "Manhattan"),
    (231, "Times Sq/Theatre District", "Manhattan"),
    (232, "TriBeCa/Civic Center", "Manhattan"),
    (233, "Two Bridges/Seward Park", "Manhattan"),
    (234, "Union Sq", "Manhattan"),
    (236, "Upper East Side North", "Manhattan"),
    (237, "Upper East Side South", "Manhattan"),
    (238, "Upper West Side North", "Manhattan"),
    (239, "Upper West Side South", "Manhattan"),
    (243, "Washington Heights North", "Manhattan"),
    (244, "Washington Heights South", "Manhattan"),
    (246, "West Chelsea/Hudson Yards", "Manhattan"),
    (249, "West Village", "Manhattan"),
    (261, "World Trade Center", "Manhattan"),
    (262, "Yorkville East", "Manhattan"),
    (263, "Yorkville West", "Manhattan"),
]

CENTROIDS = {
    1: (-74.17, 40.69), 4: (-73.98, 40.72), 7: (-73.92, 40.77),
    12: (-74.01, 40.70), 13: (-74.02, 40.71), 24: (-73.99, 40.63),
    36: (-73.94, 40.81), 37: (-73.94, 40.82), 42: (-73.97, 40.78),
    43: (-74.00, 40.75), 45: (-73.99, 40.72), 48: (-73.99, 40.76),
    50: (-74.00, 40.76), 68: (-73.99, 40.74), 74: (-73.94, 40.80),
    75: (-73.94, 40.79), 79: (-73.98, 40.73), 87: (-74.01, 40.71),
    88: (-74.01, 40.71), 90: (-73.99, 40.74), 100: (-73.99, 40.75),
    107: (-73.98, 40.74), 113: (-74.00, 40.73), 114: (-74.00, 40.73),
    125: (-74.01, 40.73), 127: (-74.00, 40.75), 128: (-73.92, 40.87),
    130: (-73.88, 40.75), 132: (-73.78, 40.64), 137: (-73.98, 40.74),
    140: (-73.87, 40.77), 141: (-73.96, 40.77), 142: (-73.96, 40.77),
    143: (-73.98, 40.77), 144: (-73.98, 40.77), 148: (-73.98, 40.72),
    151: (-73.97, 40.80), 152: (-73.91, 40.87), 153: (-74.19, 40.64),
    158: (-74.01, 40.74), 161: (-73.98, 40.75), 162: (-73.97, 40.75),
    163: (-73.98, 40.76), 164: (-73.98, 40.75), 166: (-73.96, 40.81),
    170: (-73.98, 40.75), 186: (-73.99, 40.75), 194: (-73.92, 40.79),
    202: (-73.95, 40.76), 209: (-74.00, 40.71), 211: (-74.00, 40.72),
    224: (-73.98, 40.73), 229: (-73.97, 40.76), 230: (-73.97, 40.75),
    231: (-73.99, 40.76), 232: (-74.01, 40.72), 233: (-73.99, 40.71),
    234: (-73.99, 40.74), 236: (-73.95, 40.78), 237: (-73.96, 40.77),
    238: (-73.97, 40.80), 239: (-73.97, 40.79), 243: (-73.94, 40.85),
    244: (-73.94, 40.84), 246: (-74.00, 40.75), 249: (-74.00, 40.73),
    261: (-74.01, 40.71), 262: (-73.95, 40.78), 263: (-73.95, 40.78),
}


def zone_base_demand(zone_id):
    """Return base hourly demand for a zone (higher for Midtown/airports)."""
    hot_zones = {161, 162, 163, 164, 170, 186, 231, 236, 237, 230, 234, 132, 140, 43, 48}
    med_zones = {79, 87, 88, 90, 100, 107, 113, 114, 125, 141, 142, 143, 144, 239, 238}
    if zone_id in hot_zones:
        return random.randint(800, 2000)
    if zone_id in med_zones:
        return random.randint(200, 700)
    return random.randint(20, 200)


def generate_zone_stats():
    """Generate zone_stats.json"""
    stats = []
    for zid, name, borough in ZONES:
        base = zone_base_demand(zid)
        total = base * 24 * 730  # ~2 years of hours
        stats.append({
            "zone_id": zid,
            "borough": borough,
            "zone_name": name,
            "total_trips": total,
            "avg_fare": round(random.uniform(10, 45), 2),
            "avg_total": round(random.uniform(15, 60), 2),
            "avg_distance": round(random.uniform(1.2, 12.0), 2),
            "avg_duration_min": round(random.uniform(8, 35), 1),
            "avg_speed_mph": round(random.uniform(8, 25), 1),
            "avg_tip_pct": round(random.uniform(12, 22), 1),
            "avg_passengers": round(random.uniform(1.1, 1.8), 2),
            "total_revenue": round(total * random.uniform(18, 40), 0),
            "fare_stddev": round(random.uniform(5, 20), 2),
            "median_fare": round(random.uniform(8, 35), 2),
            "median_distance": round(random.uniform(1.0, 8.0), 2),
            "peak_hour": random.choice([8, 9, 17, 18, 19, 12, 13, 22]),
        })
    return stats


def generate_daily_demand():
    """Generate daily_demand.json (daily trips per zone)."""
    data = []
    start = datetime(2023, 1, 1)
    for day_offset in range(730):  # 2 years
        date = start + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")
        dow = date.weekday()
        is_weekend = dow >= 5

        for zid, name, borough in ZONES:
            base = zone_base_demand(zid)
            daily_base = base * 24
            # Weekend/weekday modulation
            factor = 0.7 if is_weekend else 1.0
            # Seasonal modulation
            month = date.month
            if month in (6, 7, 8):
                factor *= 0.85
            elif month == 12:
                factor *= 1.15
            # Random noise
            trips = max(1, int(daily_base * factor * random.uniform(0.8, 1.2)))
            data.append({
                "pu_location_id": zid,
                "pu_borough": borough,
                "pickup_date": date_str,
                "trip_count": trips,
                "avg_fare": round(random.uniform(10, 40), 2),
                "daily_revenue": round(trips * random.uniform(18, 40), 0),
            })
    return data


def generate_od_flows():
    """Generate od_flows.json (top OD pairs)."""
    flows = []
    zone_ids = [z[0] for z in ZONES]
    for _ in range(300):
        pu = random.choice(ZONES)
        do = random.choice(ZONES)
        while do[0] == pu[0]:
            do = random.choice(ZONES)
        trips = random.randint(5000, 500000)
        flows.append({
            "pu_location_id": pu[0],
            "do_location_id": do[0],
            "pu_borough": pu[2],
            "do_borough": do[2],
            "pu_zone": pu[1],
            "do_zone": do[1],
            "trip_count": trips,
            "avg_fare": round(random.uniform(10, 50), 2),
            "avg_duration_min": round(random.uniform(8, 40), 1),
            "avg_distance": round(random.uniform(1, 15), 2),
        })
    flows.sort(key=lambda x: x["trip_count"], reverse=True)
    return flows[:300]


def generate_borough_daily():
    """Generate borough_daily.json."""
    boroughs = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
    base_trips = {"Manhattan": 80000, "Brooklyn": 15000, "Queens": 12000,
                  "Bronx": 5000, "Staten Island": 1000, "EWR": 3000}
    data = []
    start = datetime(2023, 1, 1)
    for day_offset in range(730):
        date = start + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")
        for b in boroughs:
            trips = int(base_trips[b] * random.uniform(0.7, 1.3))
            data.append({
                "pu_borough": b,
                "pickup_date": date_str,
                "trip_count": trips,
                "total_revenue": round(trips * random.uniform(20, 35), 0),
                "avg_fare": round(random.uniform(12, 30), 2),
                "avg_speed": round(random.uniform(10, 22), 1),
                "avg_distance": round(random.uniform(2, 8), 2),
                "avg_tip_pct": round(random.uniform(14, 20), 1),
            })
    return data


def generate_predictions():
    """Generate predictions.json (GBT model output)."""
    data = []
    top_zones = [161, 162, 163, 164, 170, 186, 231, 236, 237, 230,
                 234, 132, 140, 43, 48, 79, 87, 90, 100, 107]
    start = datetime(2024, 1, 1)
    for zid in top_zones:
        base = zone_base_demand(zid)
        for hour_offset in range(0, 720, 1):  # ~30 days hourly
            dt = start + timedelta(hours=hour_offset)
            hour = dt.hour
            # Time-of-day pattern
            hour_factor = 0.3 + 0.7 * math.sin(math.pi * (hour - 4) / 18) ** 2
            actual = max(1, int(base * hour_factor * random.uniform(0.7, 1.3)))
            predicted = max(1, int(base * hour_factor * random.uniform(0.85, 1.15)))
            data.append({
                "pu_location_id": zid,
                "datetime_hour": dt.strftime("%Y-%m-%d %H:00:00"),
                "hour": hour,
                "dow": dt.isoweekday(),
                "month": dt.month,
                "trip_count": actual,
                "predicted_demand": predicted,
            })
    return data


def generate_clusters():
    """Generate clusters.json (KMeans output)."""
    data = []
    for zid, name, borough in ZONES:
        base = zone_base_demand(zid)
        # Assign cluster based on zone characteristics
        if zid in (132, 140, 1):
            cid = 0  # Airport
        elif base > 800:
            cid = 1  # High-demand business
        elif base > 400:
            cid = 2  # Medium commercial
        elif borough == "Manhattan":
            cid = 3  # Residential Manhattan
        elif borough in ("Brooklyn", "Queens"):
            cid = 4  # Outer borough
        else:
            cid = 5  # Low-demand
        data.append({
            "zone_id": zid,
            "borough": borough,
            "zone_name": name,
            "total_trips": base * 24 * 730,
            "avg_fare": round(random.uniform(10, 45), 2),
            "avg_distance": round(random.uniform(1.2, 12.0), 2),
            "avg_duration_min": round(random.uniform(8, 35), 1),
            "avg_speed_mph": round(random.uniform(8, 25), 1),
            "avg_tip_pct": round(random.uniform(12, 22), 1),
            "peak_hour": random.choice([8, 9, 17, 18, 19]),
            "cluster_id": cid,
        })
    return data


def generate_anomalies():
    """Generate anomalies.json."""
    data = []
    start = datetime(2024, 1, 1)
    severities = ["critical", "high", "medium"]
    for _ in range(500):
        zone = random.choice(ZONES)
        dt = start + timedelta(hours=random.randint(0, 8760))
        direction = random.choice(["surge", "drop"])
        z_score = random.uniform(3.0, 8.0) * (1 if direction == "surge" else -1)
        baseline_mean = random.uniform(50, 500)
        baseline_std = random.uniform(10, 80)
        trip_count = max(0, int(baseline_mean + z_score * baseline_std))
        sev = "critical" if abs(z_score) >= 5 else "high" if abs(z_score) >= 4 else "medium"
        data.append({
            "pu_location_id": zone[0],
            "datetime_hour": dt.strftime("%Y-%m-%d %H:00:00"),
            "hour": dt.hour,
            "dow": dt.isoweekday(),
            "trip_count": trip_count,
            "baseline_mean": round(baseline_mean, 1),
            "baseline_std": round(baseline_std, 1),
            "z_score": round(z_score, 2),
            "severity": sev,
            "direction": direction,
        })
    data.sort(key=lambda x: abs(x["z_score"]), reverse=True)
    return data


def generate_temporal_patterns():
    """Generate temporal_patterns.json (hour x dow heatmap)."""
    data = []
    for zid, name, borough in ZONES:
        base = zone_base_demand(zid)
        for hour in range(24):
            for dow in range(1, 8):  # 1=Sun, 7=Sat
                hour_factor = 0.3 + 0.7 * math.sin(math.pi * (hour - 4) / 18) ** 2
                dow_factor = 0.75 if dow in (1, 7) else 1.0
                avg = max(0.1, base * hour_factor * dow_factor * random.uniform(0.85, 1.15))
                data.append({
                    "pu_location_id": zid,
                    "hour_of_day": hour,
                    "day_of_week": dow,
                    "avg_trips": round(avg, 1),
                    "avg_fare": round(random.uniform(10, 40), 2),
                })
    return data


def generate_overview(zone_stats, borough_daily):
    """Generate overview.json."""
    total_trips = sum(z["total_trips"] for z in zone_stats)
    total_revenue = sum(z["total_revenue"] for z in zone_stats)
    borough_totals = {}
    for b in borough_daily:
        borough_totals[b["pu_borough"]] = borough_totals.get(b["pu_borough"], 0) + b["trip_count"]
    return {
        "total_zones": len(zone_stats),
        "total_trips": total_trips,
        "total_revenue": total_revenue,
        "avg_fare": round(sum(z["avg_fare"] for z in zone_stats) / len(zone_stats), 2),
        "avg_distance": round(sum(z["avg_distance"] for z in zone_stats) / len(zone_stats), 2),
        "avg_duration_min": round(sum(z["avg_duration_min"] for z in zone_stats) / len(zone_stats), 1),
        "data_range": {"start": "2023-01", "end": "2024-12"},
        "trips_by_borough": borough_totals,
    }


def generate_taxi_zones_geojson():
    """Generate a simplified taxi_zones GeoJSON from centroids (polygon approximation)."""
    features = []
    for zid, name, borough in ZONES:
        c = CENTROIDS.get(zid)
        if not c:
            continue
        lng, lat = c
        # Create a small hexagonal polygon around the centroid
        r = 0.004  # ~400m radius
        coords = []
        for i in range(6):
            angle = math.pi / 3 * i + math.pi / 6
            coords.append([
                round(lng + r * math.cos(angle), 6),
                round(lat + r * math.sin(angle) * 0.75, 6),  # compress lat for Mercator
            ])
        coords.append(coords[0])  # close ring

        features.append({
            "type": "Feature",
            "properties": {
                "LocationID": zid,
                "Zone": name,
                "Borough": borough,
                "centroid_lng": lng,
                "centroid_lat": lat,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords],
            },
        })
    return {"type": "FeatureCollection", "features": features}


def write_json(data, name):
    path = os.path.join(OUT_DIR, f"{name}.json")
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(path) / 1024
    count = len(data) if isinstance(data, list) else "dict"
    print(f"  {name}.json: {count} records ({size_kb:.0f} KB)")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Generating demo data...\n")

    zone_stats = generate_zone_stats()
    write_json(zone_stats, "zone_stats")

    print("  daily_demand.json: generating (this is the largest file)...")
    daily_demand = generate_daily_demand()
    write_json(daily_demand, "daily_demand")

    od_flows = generate_od_flows()
    write_json(od_flows, "od_flows")

    borough_daily = generate_borough_daily()
    write_json(borough_daily, "borough_daily")

    predictions = generate_predictions()
    write_json(predictions, "predictions")

    clusters = generate_clusters()
    write_json(clusters, "clusters")

    anomalies = generate_anomalies()
    write_json(anomalies, "anomalies")

    temporal = generate_temporal_patterns()
    write_json(temporal, "temporal_patterns")

    overview = generate_overview(zone_stats, borough_daily)
    write_json(overview, "overview")

    geojson = generate_taxi_zones_geojson()
    write_json(geojson, "taxi_zones")

    total_size = sum(
        os.path.getsize(os.path.join(OUT_DIR, f))
        for f in os.listdir(OUT_DIR) if f.endswith(".json")
    ) / (1024 * 1024)

    print(f"\n  Total demo data size: {total_size:.1f} MB")
    print(f"  Output directory: {OUT_DIR}")
    print("\nDone!")


if __name__ == "__main__":
    main()

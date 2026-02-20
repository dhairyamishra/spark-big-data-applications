#!/usr/bin/env python3
"""
Convert NYC taxi zone shapefile to GeoJSON for the frontend map.
Also computes zone centroids for geohash-based features.

Usage:
    python generate_geojson.py
"""

import json
import os
import sys
import zipfile

import geopandas as gpd
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "pipeline"))
from config import LOCAL_ZONE_DIR, EXPORT_DIR


def main():
    zip_path = os.path.join(LOCAL_ZONE_DIR, "taxi_zones.zip")
    csv_path = os.path.join(LOCAL_ZONE_DIR, "taxi_zone_lookup.csv")

    if not os.path.exists(zip_path):
        print(f"ERROR: {zip_path} not found. Run download_data.py first.")
        sys.exit(1)

    # Extract shapefile
    extract_dir = os.path.join(LOCAL_ZONE_DIR, "shapefile")
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)

    # Find the .shp file
    shp_files = [f for f in os.listdir(extract_dir) if f.endswith(".shp")]
    if not shp_files:
        # Check subdirectories
        for root, dirs, files in os.walk(extract_dir):
            for f in files:
                if f.endswith(".shp"):
                    shp_files.append(os.path.join(root, f))
                    break

    if not shp_files:
        print("ERROR: No .shp file found in the zip archive.")
        sys.exit(1)

    shp_path = shp_files[0] if os.path.isabs(shp_files[0]) else os.path.join(extract_dir, shp_files[0])
    print(f"Reading shapefile: {shp_path}")
    gdf = gpd.read_file(shp_path)

    # Reproject to WGS84 (EPSG:4326) for web mapping
    gdf = gdf.to_crs(epsg=4326)

    # Load zone lookup for borough/zone names
    if os.path.exists(csv_path):
        lookup = pd.read_csv(csv_path)
        gdf = gdf.merge(lookup, left_on="LocationID", right_on="LocationID", how="left")

    # Compute centroids
    gdf["centroid_lng"] = gdf.geometry.centroid.x
    gdf["centroid_lat"] = gdf.geometry.centroid.y

    # Simplify geometry for smaller file size (tolerance in degrees ≈ 20m)
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.0002, preserve_topology=True)

    # Export GeoJSON
    os.makedirs(EXPORT_DIR, exist_ok=True)
    geojson_path = os.path.join(EXPORT_DIR, "taxi_zones.geojson")
    gdf.to_file(geojson_path, driver="GeoJSON")
    print(f"  ✓ GeoJSON written to {geojson_path}")

    # Export centroids as JSON (for pipeline geohash features)
    centroids = gdf[["LocationID", "centroid_lat", "centroid_lng"]].to_dict(orient="records")
    centroids_path = os.path.join(EXPORT_DIR, "zone_centroids.json")
    with open(centroids_path, "w") as f:
        json.dump(centroids, f)
    print(f"  ✓ Centroids written to {centroids_path}")

    # Stats
    file_size = os.path.getsize(geojson_path) / 1024
    print(f"\n  Zones: {len(gdf)}")
    print(f"  GeoJSON size: {file_size:.0f} KB")


if __name__ == "__main__":
    main()

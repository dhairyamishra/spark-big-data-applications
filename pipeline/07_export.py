#!/usr/bin/env python3
"""
Step 7: Export gold-layer Parquet → JSON artifacts for the web app.

Converts Spark DataFrames to compact JSON files that the FastAPI
backend loads into memory at startup.

Usage:
    spark-submit 07_export.py
"""

import json
import os

from pyspark.sql import functions as F

from utils import get_spark
from config import (
    HDFS_GOLD_ZONE_STATS,
    HDFS_GOLD_HOURLY_DEMAND,
    HDFS_GOLD_OD_FLOWS,
    HDFS_GOLD_BOROUGH_DAILY,
    HDFS_GOLD_TEMPORAL_PATTERNS,
    HDFS_GOLD_PREDICTIONS,
    HDFS_GOLD_CLUSTERS,
    HDFS_GOLD_ANOMALIES,
    EXPORT_DIR,
)


def export_to_json(spark, hdfs_path: str, output_name: str, limit: int = None):
    """Read Parquet from HDFS, convert to JSON, write to export dir."""
    print(f"\n  Exporting {output_name}...")

    try:
        df = spark.read.parquet(hdfs_path)
    except Exception as e:
        print(f"  [SKIP] {hdfs_path}: {e}")
        return

    if limit:
        df = df.limit(limit)

    # Convert timestamps to ISO strings for JSON serialization
    for field in df.schema.fields:
        if "timestamp" in str(field.dataType).lower() or "date" in str(field.dataType).lower():
            df = df.withColumn(field.name, F.col(field.name).cast("string"))

    # Collect to driver and write as JSON
    rows = df.toJSON().collect()
    data = [json.loads(r) for r in rows]

    os.makedirs(EXPORT_DIR, exist_ok=True)
    out_path = os.path.join(EXPORT_DIR, f"{output_name}.json")
    with open(out_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    size_kb = os.path.getsize(out_path) / 1024
    print(f"  ✓ {output_name}.json ({len(data):,} records, {size_kb:.0f} KB)")


def export_overview_stats(spark):
    """Compute and export global summary statistics."""
    print("\n  Computing overview stats...")

    zone_stats = spark.read.parquet(HDFS_GOLD_ZONE_STATS)

    overview = {
        "total_zones": zone_stats.count(),
        "total_trips": int(zone_stats.agg(F.sum("total_trips")).collect()[0][0] or 0),
        "total_revenue": float(zone_stats.agg(F.sum("total_revenue")).collect()[0][0] or 0),
        "avg_fare": float(zone_stats.agg(F.avg("avg_fare")).collect()[0][0] or 0),
        "avg_distance": float(zone_stats.agg(F.avg("avg_distance")).collect()[0][0] or 0),
        "avg_duration_min": float(zone_stats.agg(F.avg("avg_duration_min")).collect()[0][0] or 0),
        "data_range": {"start": "2023-01", "end": "2024-12"},
    }

    # Trips by borough
    borough_totals = (
        zone_stats
        .groupBy("borough")
        .agg(F.sum("total_trips").alias("trips"))
        .orderBy(F.desc("trips"))
        .collect()
    )
    overview["trips_by_borough"] = {
        row["borough"]: int(row["trips"]) for row in borough_totals if row["borough"]
    }

    out_path = os.path.join(EXPORT_DIR, "overview.json")
    with open(out_path, "w") as f:
        json.dump(overview, f, separators=(",", ":"))

    print(f"  ✓ overview.json")
    return overview


def main():
    spark = get_spark("NYC-Mobility-Export")
    os.makedirs(EXPORT_DIR, exist_ok=True)

    # Export each gold-layer dataset
    exports = [
        (HDFS_GOLD_ZONE_STATS, "zone_stats", None),
        (HDFS_GOLD_OD_FLOWS, "od_flows", 500),
        (HDFS_GOLD_BOROUGH_DAILY, "borough_daily", None),
        (HDFS_GOLD_TEMPORAL_PATTERNS, "temporal_patterns", None),
        (HDFS_GOLD_PREDICTIONS, "predictions", 50000),
        (HDFS_GOLD_CLUSTERS, "clusters", None),
        (HDFS_GOLD_ANOMALIES, "anomalies", 2000),
    ]

    for hdfs_path, name, limit in exports:
        export_to_json(spark, hdfs_path, name, limit)

    # Hourly demand — export aggregated to daily for size management
    print("\n  Exporting hourly_demand (daily aggregation for size)...")
    try:
        hourly = spark.read.parquet(HDFS_GOLD_HOURLY_DEMAND)
        daily_demand = (
            hourly
            .groupBy("pu_location_id", "pu_borough", "pickup_date")
            .agg(
                F.sum("trip_count").alias("trip_count"),
                F.avg("avg_fare").alias("avg_fare"),
                F.sum("hour_revenue").alias("daily_revenue"),
            )
            .withColumn("pickup_date", F.col("pickup_date").cast("string"))
            .orderBy("pickup_date", "pu_location_id")
        )
        rows = daily_demand.toJSON().collect()
        data = [json.loads(r) for r in rows]
        out_path = os.path.join(EXPORT_DIR, "daily_demand.json")
        with open(out_path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        print(f"  ✓ daily_demand.json ({len(data):,} records)")
    except Exception as e:
        print(f"  [SKIP] hourly_demand: {e}")

    # Overview stats
    export_overview_stats(spark)

    spark.stop()
    print("\n✓ Export complete. JSON artifacts ready in", EXPORT_DIR)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Step 2: Clean and validate bronze data → write to silver layer.

- Enforces a unified schema across Yellow, Green, and HVFHV
- Removes nulls, duplicates, outliers, impossible values
- Writes a single unified `silver/trips` Parquet dataset

Usage:
    spark-submit 02_clean.py
"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from utils import get_spark, log_df_info, write_partitioned_parquet
from config import (
    HDFS_BRONZE_YELLOW, HDFS_BRONZE_GREEN, HDFS_BRONZE_HVFHV,
    HDFS_SILVER_TRIPS, UNIFIED_COLUMNS, LOCAL_ZONE_DIR,
)

# ---------------------------------------------------------------------------
# Cleaning thresholds
# ---------------------------------------------------------------------------
MIN_FARE = 0.0
MAX_FARE = 1000.0
MIN_DISTANCE = 0.01
MAX_DISTANCE = 200.0
MIN_DURATION_SEC = 30
MAX_DURATION_SEC = 6 * 3600  # 6 hours
MIN_YEAR = 2023
MAX_YEAR = 2024


def load_and_normalize(spark, path: str, taxi_type: str):
    """Load a bronze dataset and select unified columns."""
    print(f"\n  Loading {taxi_type} from {path}...")

    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"  [SKIP] Could not read {path}: {e}")
        return None

    # Select only the columns we need (all types already have these from ingestion)
    available = set(df.columns)
    select_cols = [c for c in UNIFIED_COLUMNS + ["year", "month"] if c in available]

    df = df.select(*select_cols)

    # Ensure consistent types
    df = (
        df
        .withColumn("pu_location_id",
                     F.col("pu_location_id").cast(IntegerType()) if "pu_location_id" in available
                     else F.col("PULocationID").cast(IntegerType()))
        .withColumn("do_location_id",
                     F.col("do_location_id").cast(IntegerType()) if "do_location_id" in available
                     else F.col("DOLocationID").cast(IntegerType()))
    )

    return df


def unify_schemas(spark):
    """Load all bronze taxi types and union into a single DataFrame."""
    sources = [
        (HDFS_BRONZE_YELLOW, "yellow"),
        (HDFS_BRONZE_GREEN, "green"),
        (HDFS_BRONZE_HVFHV, "hvfhv"),
    ]

    dfs = []
    for path, ttype in sources:
        df = load_and_normalize(spark, path, ttype)
        if df is not None:
            dfs.append(df)

    if not dfs:
        raise RuntimeError("No bronze data found. Run 01_ingest.py first.")

    # Union all with unified column set
    unified = dfs[0]
    for df in dfs[1:]:
        # Align columns before union
        for col in unified.columns:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None))
        for col in df.columns:
            if col not in unified.columns:
                unified = unified.withColumn(col, F.lit(None))
        # Select in same order
        df = df.select(unified.columns)
        unified = unified.unionByName(df, allowMissingColumns=True)

    return unified


def apply_cleaning_rules(df):
    """Apply data quality filters and compute derived cleaning columns."""

    initial_count = df.count()
    print(f"\n  Initial row count: {initial_count:,}")

    # 1. Remove rows with null pickup/dropoff location
    df = df.filter(
        F.col("pu_location_id").isNotNull()
        & F.col("do_location_id").isNotNull()
        & (F.col("pu_location_id") > 0)
        & (F.col("do_location_id") > 0)
        & (F.col("pu_location_id") <= 265)
        & (F.col("do_location_id") <= 265)
    )

    # 2. Remove null/invalid timestamps
    df = df.filter(
        F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & (F.col("dropoff_datetime") > F.col("pickup_datetime"))
    )

    # 3. Filter to valid date range
    df = df.filter(
        (F.year("pickup_datetime") >= MIN_YEAR)
        & (F.year("pickup_datetime") <= MAX_YEAR)
    )

    # 4. Compute trip duration in seconds
    df = df.withColumn(
        "trip_duration_sec",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
    )

    # 5. Filter duration
    df = df.filter(
        (F.col("trip_duration_sec") >= MIN_DURATION_SEC)
        & (F.col("trip_duration_sec") <= MAX_DURATION_SEC)
    )

    # 6. Filter fares
    df = df.filter(
        (F.coalesce(F.col("fare_amount"), F.lit(0)) >= MIN_FARE)
        & (F.coalesce(F.col("fare_amount"), F.lit(0)) <= MAX_FARE)
    )

    # 7. Filter distance
    df = df.filter(
        (F.coalesce(F.col("trip_distance"), F.lit(0)) >= MIN_DISTANCE)
        & (F.coalesce(F.col("trip_distance"), F.lit(0)) <= MAX_DISTANCE)
    )

    # 8. Deduplicate
    df = df.dropDuplicates(["taxi_type", "pickup_datetime", "dropoff_datetime",
                            "pu_location_id", "do_location_id", "fare_amount"])

    # 9. Coalesce nulls for numeric fields
    df = (
        df
        .withColumn("passenger_count", F.coalesce(F.col("passenger_count"), F.lit(1)))
        .withColumn("tip_amount", F.coalesce(F.col("tip_amount"), F.lit(0)))
        .withColumn("total_amount", F.coalesce(F.col("total_amount"), F.col("fare_amount")))
    )

    final_count = df.count()
    dropped = initial_count - final_count
    pct = (dropped / initial_count * 100) if initial_count > 0 else 0
    print(f"  Rows after cleaning: {final_count:,}  (dropped {dropped:,} = {pct:.1f}%)")

    return df


def enrich_with_zones(spark, df):
    """Broadcast-join zone lookup to add borough and zone names."""
    import os
    csv_path = os.path.join(LOCAL_ZONE_DIR, "taxi_zone_lookup.csv")
    zone_df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # Pickup zone enrichment
    pu_zones = zone_df.select(
        F.col("LocationID").alias("pu_location_id"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
    )

    # Dropoff zone enrichment
    do_zones = zone_df.select(
        F.col("LocationID").alias("do_location_id"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone"),
    )

    df = df.join(F.broadcast(pu_zones), on="pu_location_id", how="left")
    df = df.join(F.broadcast(do_zones), on="do_location_id", how="left")

    return df


def main():
    spark = get_spark("NYC-Mobility-Clean")

    # Load and unify all bronze data
    print("Step 1: Loading and unifying bronze data...")
    unified = unify_schemas(spark)
    log_df_info(unified, "Unified Bronze")

    # Apply cleaning rules
    print("\nStep 2: Applying cleaning rules...")
    cleaned = apply_cleaning_rules(unified)

    # Enrich with zone names
    print("\nStep 3: Enriching with zone lookup (broadcast join)...")
    enriched = enrich_with_zones(spark, cleaned)
    log_df_info(enriched, "Silver Trips (cleaned + enriched)")

    # Write to silver layer
    print("\nStep 4: Writing to silver layer...")
    write_partitioned_parquet(enriched, HDFS_SILVER_TRIPS, ["year", "month"])

    spark.stop()
    print("\n✓ Cleaning complete. Silver layer ready.")


if __name__ == "__main__":
    main()

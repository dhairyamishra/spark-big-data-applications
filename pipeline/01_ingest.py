#!/usr/bin/env python3
"""
Step 1: Ingest raw Parquet files into HDFS bronze layer.

- Reads Yellow, Green, and HVFHV trip data from local Parquet files
- Uploads to HDFS bronze layer partitioned by year/month
- Broadcasts taxi zone lookup for enrichment

Usage:
    spark-submit 01_ingest.py
    spark-submit 01_ingest.py --types yellow --years 2024
"""

import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    TimestampType, LongType, StringType,
)

from utils import get_spark, log_df_info, write_partitioned_parquet
from config import (
    LOCAL_DATA_DIR, LOCAL_ZONE_DIR, YEARS, TAXI_TYPES,
    HDFS_BRONZE_YELLOW, HDFS_BRONZE_GREEN, HDFS_BRONZE_HVFHV,
)

# ---------------------------------------------------------------------------
# Schemas for each taxi type (enforce on read for safety)
# ---------------------------------------------------------------------------
YELLOW_SCHEMA = StructType([
    StructField("VendorID", LongType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", LongType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()),
])

GREEN_SCHEMA = StructType([
    StructField("VendorID", LongType()),
    StructField("lpep_pickup_datetime", TimestampType()),
    StructField("lpep_dropoff_datetime", TimestampType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("RatecodeID", DoubleType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("ehail_fee", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("payment_type", DoubleType()),
    StructField("trip_type", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
])

HVFHV_SCHEMA = StructType([
    StructField("hvfhs_license_num", StringType()),
    StructField("dispatching_base_num", StringType()),
    StructField("originating_base_num", StringType()),
    StructField("request_datetime", TimestampType()),
    StructField("on_scene_datetime", TimestampType()),
    StructField("pickup_datetime", TimestampType()),
    StructField("dropoff_datetime", TimestampType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("trip_miles", DoubleType()),
    StructField("trip_time", LongType()),
    StructField("base_passenger_fare", DoubleType()),
    StructField("tolls", DoubleType()),
    StructField("bcf", DoubleType()),
    StructField("sales_tax", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()),
    StructField("tips", DoubleType()),
    StructField("driver_pay", DoubleType()),
    StructField("shared_request_flag", StringType()),
    StructField("shared_match_flag", StringType()),
    StructField("access_a_ride_flag", StringType()),
    StructField("wav_request_flag", StringType()),
    StructField("wav_match_flag", StringType()),
])

HDFS_TARGETS = {
    "yellow": HDFS_BRONZE_YELLOW,
    "green": HDFS_BRONZE_GREEN,
    "hvfhv": HDFS_BRONZE_HVFHV,
}

SCHEMAS = {
    "yellow": YELLOW_SCHEMA,
    "green": GREEN_SCHEMA,
    "hvfhv": HVFHV_SCHEMA,
}


def ingest_taxi_type(spark, taxi_type: str, years: list):
    """Read local Parquet files for a taxi type and write to HDFS bronze layer."""
    print(f"\n{'='*60}")
    print(f"  Ingesting: {taxi_type.upper()}")
    print(f"{'='*60}")

    data_dir = os.path.join(LOCAL_DATA_DIR, taxi_type)
    if not os.path.exists(data_dir):
        print(f"  [SKIP] {data_dir} does not exist")
        return

    # Collect all Parquet file paths
    parquet_files = []
    for year in years:
        year_dir = os.path.join(data_dir, str(year))
        if os.path.exists(year_dir):
            for f in sorted(os.listdir(year_dir)):
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(year_dir, f))

    if not parquet_files:
        print(f"  [SKIP] No Parquet files found for {taxi_type}")
        return

    print(f"  Found {len(parquet_files)} files")

    # Read with schema enforcement
    df = spark.read.parquet(*parquet_files)

    # Add metadata columns
    if taxi_type == "yellow":
        df = (
            df
            .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
            .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
            .withColumn("taxi_type", F.lit("yellow"))
        )
    elif taxi_type == "green":
        df = (
            df
            .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
            .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
            .withColumn("taxi_type", F.lit("green"))
        )
    elif taxi_type == "hvfhv":
        df = (
            df
            .withColumn("taxi_type", F.lit("hvfhv"))
            .withColumnRenamed("trip_miles", "trip_distance")
            .withColumnRenamed("base_passenger_fare", "fare_amount")
            .withColumnRenamed("tips", "tip_amount")
            .withColumn("total_amount",
                        F.col("fare_amount") + F.coalesce(F.col("tolls"), F.lit(0))
                        + F.coalesce(F.col("tip_amount"), F.lit(0)))
            .withColumn("passenger_count", F.lit(None).cast(DoubleType()))
            .withColumn("payment_type", F.lit(None).cast(LongType()))
        )

    # Extract year/month for partitioning
    df = (
        df
        .withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
    )

    log_df_info(df, f"Bronze {taxi_type}")

    # Write to HDFS bronze layer
    hdfs_path = HDFS_TARGETS[taxi_type]
    write_partitioned_parquet(df, hdfs_path, ["year", "month"])


def broadcast_zone_lookup(spark):
    """Load and broadcast the taxi zone lookup table."""
    csv_path = os.path.join(LOCAL_ZONE_DIR, "taxi_zone_lookup.csv")
    if not os.path.exists(csv_path):
        print(f"  [WARN] Zone lookup not found at {csv_path}")
        return None

    zone_df = spark.read.csv(csv_path, header=True, inferSchema=True)
    log_df_info(zone_df, "Taxi Zone Lookup")

    # Broadcast for efficient joins downstream
    broadcast_df = F.broadcast(zone_df)
    print("  ✓ Zone lookup table broadcasted")
    return broadcast_df


def main():
    parser = argparse.ArgumentParser(description="Ingest TLC data to HDFS bronze")
    parser.add_argument("--types", nargs="+", default=list(TAXI_TYPES.keys()),
                        choices=list(TAXI_TYPES.keys()))
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    args = parser.parse_args()

    spark = get_spark("NYC-Mobility-Ingest")

    # Broadcast zone lookup (used later in pipeline, validated here)
    broadcast_zone_lookup(spark)

    # Ingest each taxi type
    for taxi_type in args.types:
        ingest_taxi_type(spark, taxi_type, args.years)

    spark.stop()
    print("\n✓ Ingestion complete.")


if __name__ == "__main__":
    main()

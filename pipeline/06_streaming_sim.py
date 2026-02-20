#!/usr/bin/env python3
"""
Step 6: Structured Streaming simulation.

Replays one day of historical trip data as a stream to demonstrate:
  - Spark Structured Streaming
  - Sliding window aggregations
  - Watermarking for late data
  - Output to console/memory sink

Usage:
    spark-submit 06_streaming_sim.py
    spark-submit 06_streaming_sim.py --date 2024-06-15
"""

import argparse
import os
import shutil

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    TimestampType, StringType,
)

from utils import get_spark
from config import HDFS_SILVER_ENRICHED


STREAM_INPUT_DIR = "data/streaming_input"
STREAM_CHECKPOINT = "data/streaming_checkpoint"


def prepare_streaming_source(spark, target_date: str):
    """
    Extract one day of data and write as small Parquet files
    to simulate a streaming source (file-based stream).
    """
    print(f"\n  Preparing streaming source for {target_date}...")

    # Clean previous run
    for d in [STREAM_INPUT_DIR, STREAM_CHECKPOINT]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    # Read one day from silver enriched
    df = spark.read.parquet(HDFS_SILVER_ENRICHED)
    day_data = df.filter(F.col("pickup_date") == target_date)

    count = day_data.count()
    print(f"  Trips on {target_date}: {count:,}")

    if count == 0:
        print("  [WARN] No data for this date. Try a different date.")
        return False

    # Select minimal columns for streaming
    stream_df = day_data.select(
        "pickup_datetime",
        "pu_location_id",
        "do_location_id",
        "pu_borough",
        "fare_amount",
        "trip_distance",
        "trip_duration_sec",
    )

    # Write as multiple small files (simulates batched arrival)
    stream_df.repartition(24).write.mode("overwrite").parquet(STREAM_INPUT_DIR)
    print(f"  ✓ Wrote {24} micro-batch files to {STREAM_INPUT_DIR}")
    return True


def run_streaming_pipeline(spark):
    """
    Run Structured Streaming: read from file source, compute
    15-minute windowed demand aggregations with watermarking.
    """
    print("\n  Starting Structured Streaming pipeline...")

    # Schema for the stream
    schema = StructType([
        StructField("pickup_datetime", TimestampType()),
        StructField("pu_location_id", IntegerType()),
        StructField("do_location_id", IntegerType()),
        StructField("pu_borough", StringType()),
        StructField("fare_amount", DoubleType()),
        StructField("trip_distance", DoubleType()),
        StructField("trip_duration_sec", IntegerType()),
    ])

    # Read stream from Parquet files
    stream_df = (
        spark.readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 2)  # Process 2 files per micro-batch
        .parquet(STREAM_INPUT_DIR)
    )

    print("  Stream schema:")
    stream_df.printSchema()

    # ── Streaming Aggregation ──
    # 15-minute tumbling window with 5-minute watermark for late data
    windowed = (
        stream_df
        .withWatermark("pickup_datetime", "5 minutes")
        .groupBy(
            F.window("pickup_datetime", "15 minutes"),
            "pu_location_id",
            "pu_borough",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("fare_amount").alias("window_revenue"),
        )
    )

    # Flatten window struct
    output = (
        windowed
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "pu_location_id",
            "pu_borough",
            "trip_count",
            "avg_fare",
            "avg_distance",
            "window_revenue",
        )
    )

    # Write to console (for demo)
    query = (
        output.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", STREAM_CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("\n  ── Streaming output (Ctrl+C to stop) ──\n")
    query.awaitTermination(timeout=60)  # Run for 60 seconds max in demo
    query.stop()
    print("\n  ✓ Streaming demo complete.")


def main():
    parser = argparse.ArgumentParser(description="Structured Streaming demo")
    parser.add_argument("--date", default="2024-06-15",
                        help="Date to replay (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark("NYC-Mobility-Streaming")

    if prepare_streaming_source(spark, args.date):
        run_streaming_pipeline(spark)

    spark.stop()


if __name__ == "__main__":
    main()

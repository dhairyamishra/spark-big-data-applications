#!/usr/bin/env python3
"""
Step 4: Compute analytics aggregations for the dashboard → gold layer.

Produces:
  - zone_stats:        Per-zone summary (trips, avg fare, avg duration, peak hours)
  - hourly_demand:     Trips per zone per hour (time series)
  - od_flows:          Top 500 OD pairs by volume
  - borough_daily:     Daily borough-level summary
  - temporal_patterns: Hour × day_of_week heatmap data per zone

Usage:
    spark-submit 04_analytics.py
"""

from pyspark.sql import functions as F
from pyspark.sql import Window

from utils import get_spark, log_df_info
from config import (
    HDFS_SILVER_ENRICHED,
    HDFS_GOLD_ZONE_STATS,
    HDFS_GOLD_HOURLY_DEMAND,
    HDFS_GOLD_OD_FLOWS,
    HDFS_GOLD_BOROUGH_DAILY,
    HDFS_GOLD_TEMPORAL_PATTERNS,
)


def compute_zone_stats(df):
    """Per-zone aggregate statistics."""
    print("\n  Computing zone-level statistics...")

    zone_stats = (
        df
        .groupBy("pu_location_id", "pu_borough", "pu_zone")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("total_amount").alias("avg_total"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("speed_mph").alias("avg_speed_mph"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
            F.avg("passenger_count").alias("avg_passengers"),
            F.sum("total_amount").alias("total_revenue"),
            F.stddev("fare_amount").alias("fare_stddev"),
            F.expr("percentile_approx(fare_amount, 0.5)").alias("median_fare"),
            F.expr("percentile_approx(trip_distance, 0.5)").alias("median_distance"),
        )
        .withColumnRenamed("pu_location_id", "zone_id")
        .withColumnRenamed("pu_borough", "borough")
        .withColumnRenamed("pu_zone", "zone_name")
    )

    # Peak hour per zone
    hourly_counts = (
        df
        .groupBy("pu_location_id", "hour_of_day")
        .agg(F.count("*").alias("hour_trips"))
    )
    w = Window.partitionBy("pu_location_id").orderBy(F.desc("hour_trips"))
    peak_hours = (
        hourly_counts
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .select(
            F.col("pu_location_id").alias("zone_id"),
            F.col("hour_of_day").alias("peak_hour"),
        )
    )

    zone_stats = zone_stats.join(peak_hours, on="zone_id", how="left")

    log_df_info(zone_stats, "Zone Stats")
    zone_stats.write.mode("overwrite").parquet(HDFS_GOLD_ZONE_STATS)
    print(f"  ✓ Written to {HDFS_GOLD_ZONE_STATS}")
    return zone_stats


def compute_hourly_demand(df):
    """Hourly trip counts per zone — the core time series for the dashboard."""
    print("\n  Computing hourly demand time series...")

    hourly = (
        df
        .groupBy("pu_location_id", "pu_borough", "pickup_date", "hour_of_day", "year", "month")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.sum("total_amount").alias("hour_revenue"),
        )
        .withColumn(
            "datetime_hour",
            F.to_timestamp(
                F.concat(
                    F.col("pickup_date").cast("string"),
                    F.lit(" "),
                    F.lpad(F.col("hour_of_day").cast("string"), 2, "0"),
                    F.lit(":00:00")
                )
            )
        )
    )

    log_df_info(hourly, "Hourly Demand")
    hourly.write.mode("overwrite").partitionBy("year", "month").parquet(HDFS_GOLD_HOURLY_DEMAND)
    print(f"  ✓ Written to {HDFS_GOLD_HOURLY_DEMAND}")
    return hourly


def compute_od_flows(df, top_n=500):
    """Top N origin-destination pairs by trip volume."""
    print(f"\n  Computing top {top_n} OD flows...")

    od = (
        df
        .groupBy(
            "pu_location_id", "do_location_id",
            "pu_borough", "do_borough",
            "pu_zone", "do_zone",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("trip_distance").alias("avg_distance"),
        )
        .orderBy(F.desc("trip_count"))
        .limit(top_n)
    )

    log_df_info(od, f"OD Flows (top {top_n})")
    od.write.mode("overwrite").parquet(HDFS_GOLD_OD_FLOWS)
    print(f"  ✓ Written to {HDFS_GOLD_OD_FLOWS}")
    return od


def compute_borough_daily(df):
    """Daily borough-level summary stats."""
    print("\n  Computing borough daily summary...")

    borough = (
        df
        .groupBy("pu_borough", "pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("speed_mph").alias("avg_speed"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("tip_percentage").alias("avg_tip_pct"),
        )
        .orderBy("pickup_date", "pu_borough")
    )

    log_df_info(borough, "Borough Daily")
    borough.write.mode("overwrite").parquet(HDFS_GOLD_BOROUGH_DAILY)
    print(f"  ✓ Written to {HDFS_GOLD_BOROUGH_DAILY}")
    return borough


def compute_temporal_patterns(df):
    """Hour × day_of_week heatmap aggregation per zone."""
    print("\n  Computing temporal patterns (hour × day_of_week)...")

    patterns = (
        df
        .groupBy("pu_location_id", "hour_of_day", "day_of_week")
        .agg(
            F.count("*").alias("avg_trips"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )

    # Normalize to weekly averages
    weeks_total = (
        df.select(F.countDistinct("week_of_year", "year")).collect()[0][0]
    )
    if weeks_total and weeks_total > 0:
        patterns = patterns.withColumn(
            "avg_trips", F.col("avg_trips") / F.lit(weeks_total)
        )

    log_df_info(patterns, "Temporal Patterns")
    patterns.write.mode("overwrite").parquet(HDFS_GOLD_TEMPORAL_PATTERNS)
    print(f"  ✓ Written to {HDFS_GOLD_TEMPORAL_PATTERNS}")
    return patterns


def main():
    spark = get_spark("NYC-Mobility-Analytics")

    print("Loading silver enriched trips...")
    df = spark.read.parquet(HDFS_SILVER_ENRICHED)
    df.cache()
    log_df_info(df, "Silver Enriched (input)")

    compute_zone_stats(df)
    compute_hourly_demand(df)
    compute_od_flows(df)
    compute_borough_daily(df)
    compute_temporal_patterns(df)

    df.unpersist()
    spark.stop()
    print("\n✓ Analytics aggregation complete. Gold layer ready.")


if __name__ == "__main__":
    main()

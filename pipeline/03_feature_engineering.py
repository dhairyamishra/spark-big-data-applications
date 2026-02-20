#!/usr/bin/env python3
"""
Step 3: Feature engineering on silver trips → silver enriched layer.

- Temporal features: hour, day_of_week, is_weekend, is_holiday, season
- Trip features: speed_mph, fare_per_mile, fare_per_minute, tip_percentage
- Lag features: demand at t-1h, t-24h, t-168h per zone (window functions)
- Rolling features: 7-day rolling avg demand per zone

Usage:
    spark-submit 03_feature_engineering.py
"""

from pyspark.sql import functions as F
from pyspark.sql import Window

from utils import get_spark, log_df_info, write_partitioned_parquet
from config import HDFS_SILVER_TRIPS, HDFS_SILVER_ENRICHED, US_HOLIDAYS


def add_temporal_features(df):
    """Extract time-based features from pickup_datetime."""
    print("  Adding temporal features...")

    # Core time dimensions
    df = (
        df
        .withColumn("hour_of_day", F.hour("pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("pickup_datetime"))  # 1=Sun, 7=Sat
        .withColumn("day_of_month", F.dayofmonth("pickup_datetime"))
        .withColumn("week_of_year", F.weekofyear("pickup_datetime"))
        .withColumn("is_weekend",
                     F.when(F.dayofweek("pickup_datetime").isin(1, 7), 1).otherwise(0))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
    )

    # Season (1=Winter, 2=Spring, 3=Summer, 4=Fall)
    df = df.withColumn(
        "season",
        F.when(F.col("month").isin(12, 1, 2), 1)
        .when(F.col("month").isin(3, 4, 5), 2)
        .when(F.col("month").isin(6, 7, 8), 3)
        .otherwise(4)
    )

    # Holiday flag
    holiday_list = [F.lit(h) for h in US_HOLIDAYS]
    df = df.withColumn(
        "is_holiday",
        F.when(F.col("pickup_date").cast("string").isin(US_HOLIDAYS), 1).otherwise(0)
    )

    # Time period buckets
    df = df.withColumn(
        "time_period",
        F.when((F.col("hour_of_day") >= 6) & (F.col("hour_of_day") < 10), "morning_rush")
        .when((F.col("hour_of_day") >= 10) & (F.col("hour_of_day") < 16), "midday")
        .when((F.col("hour_of_day") >= 16) & (F.col("hour_of_day") < 20), "evening_rush")
        .when((F.col("hour_of_day") >= 20) & (F.col("hour_of_day") < 24), "night")
        .otherwise("late_night")
    )

    return df


def add_trip_features(df):
    """Compute derived trip metrics."""
    print("  Adding trip features...")

    trip_duration_min = F.col("trip_duration_sec") / 60.0

    df = (
        df
        # Speed in mph
        .withColumn(
            "speed_mph",
            F.when(
                (F.col("trip_duration_sec") > 0) & (F.col("trip_distance") > 0),
                F.col("trip_distance") / (F.col("trip_duration_sec") / 3600.0)
            ).otherwise(None)
        )
        # Fare efficiency
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0,
                   F.col("fare_amount") / F.col("trip_distance")).otherwise(None)
        )
        .withColumn(
            "fare_per_minute",
            F.when(trip_duration_min > 0,
                   F.col("fare_amount") / trip_duration_min).otherwise(None)
        )
        # Tip percentage
        .withColumn(
            "tip_percentage",
            F.when(F.col("fare_amount") > 0,
                   (F.col("tip_amount") / F.col("fare_amount")) * 100).otherwise(0)
        )
        # Trip duration in minutes (convenience)
        .withColumn("trip_duration_min", trip_duration_min)
    )

    # Cap unreasonable speeds (data quality catch)
    df = df.withColumn(
        "speed_mph",
        F.when(F.col("speed_mph") > 100, None).otherwise(F.col("speed_mph"))
    )

    return df


def add_lag_features(spark, df):
    """
    Compute zone-hour level lag features using window functions.
    This is the most Spark-intensive step — demonstrates mastery of Window API.
    """
    print("  Computing zone-hour demand aggregates...")

    # First, compute hourly demand per zone
    zone_hour_demand = (
        df
        .groupBy("pu_location_id", "pickup_date", "hour_of_day")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("speed_mph").alias("avg_speed"),
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

    # Window: order by datetime_hour, partition by zone
    zone_window = Window.partitionBy("pu_location_id").orderBy("datetime_hour")

    print("  Adding lag features (t-1h, t-24h, t-168h)...")

    # Lag features — demand at previous time steps
    zone_hour_demand = (
        zone_hour_demand
        .withColumn("demand_lag_1h", F.lag("trip_count", 1).over(zone_window))
        .withColumn("demand_lag_24h", F.lag("trip_count", 24).over(zone_window))
        .withColumn("demand_lag_168h", F.lag("trip_count", 168).over(zone_window))
    )

    # Rolling 7-day average (168 hours)
    rolling_window = (
        Window.partitionBy("pu_location_id")
        .orderBy("datetime_hour")
        .rowsBetween(-168, -1)
    )

    print("  Adding 7-day rolling average demand...")
    zone_hour_demand = zone_hour_demand.withColumn(
        "demand_rolling_7d_avg",
        F.avg("trip_count").over(rolling_window)
    )

    # Ratio features
    zone_hour_demand = zone_hour_demand.withColumn(
        "demand_vs_rolling_ratio",
        F.when(F.col("demand_rolling_7d_avg") > 0,
               F.col("trip_count") / F.col("demand_rolling_7d_avg")).otherwise(1.0)
    )

    # Cache for reuse
    zone_hour_demand.cache()
    print(f"  Zone-hour demand rows: {zone_hour_demand.count():,}")

    return df, zone_hour_demand


def main():
    spark = get_spark("NYC-Mobility-Features")

    # Load silver trips
    print("Loading silver trips...")
    df = spark.read.parquet(HDFS_SILVER_TRIPS)
    log_df_info(df, "Silver Trips (input)")

    # Add temporal features
    df = add_temporal_features(df)

    # Add trip features
    df = add_trip_features(df)

    # Write enriched trips
    print("\nWriting enriched silver trips...")
    write_partitioned_parquet(df, HDFS_SILVER_ENRICHED, ["year", "month"])

    # Compute and save zone-hour demand with lag features
    df_enriched, zone_hour_demand = add_lag_features(spark, df)

    # Save zone-hour demand table (used by ML and analytics steps)
    zone_hour_path = HDFS_SILVER_ENRICHED + "_zone_hour_demand"
    (
        zone_hour_demand.write
        .mode("overwrite")
        .parquet(zone_hour_path)
    )
    print(f"  ✓ Zone-hour demand written to {zone_hour_path}")

    spark.stop()
    print("\n✓ Feature engineering complete.")


if __name__ == "__main__":
    main()

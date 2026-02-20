"""
Central configuration for the NYC Urban Mobility Spark pipeline.
All paths, constants, and Spark settings live here.
"""

# ---------------------------------------------------------------------------
# Data source URLs
# ---------------------------------------------------------------------------
TLC_CDN = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

YEARS = [2023, 2024]
MONTHS = list(range(1, 13))

TAXI_TYPES = {
    "yellow": f"{TLC_CDN}/yellow_tripdata",
    "green": f"{TLC_CDN}/green_tripdata",
    "hvfhv": f"{TLC_CDN}/fhvhv_tripdata",
}

# ---------------------------------------------------------------------------
# Local raw-data directory (before HDFS ingest)
# ---------------------------------------------------------------------------
LOCAL_DATA_DIR = "data/raw"
LOCAL_ZONE_DIR = "data/zones"

# ---------------------------------------------------------------------------
# HDFS paths  (bronze → silver → gold medallion architecture)
# ---------------------------------------------------------------------------
HDFS_BASE = "hdfs:///urban_mobility"
HDFS_BRONZE = f"{HDFS_BASE}/bronze"
HDFS_SILVER = f"{HDFS_BASE}/silver"
HDFS_GOLD = f"{HDFS_BASE}/gold"

HDFS_BRONZE_YELLOW = f"{HDFS_BRONZE}/yellow"
HDFS_BRONZE_GREEN = f"{HDFS_BRONZE}/green"
HDFS_BRONZE_HVFHV = f"{HDFS_BRONZE}/hvfhv"

HDFS_SILVER_TRIPS = f"{HDFS_SILVER}/trips"
HDFS_SILVER_ENRICHED = f"{HDFS_SILVER}/enriched"

HDFS_GOLD_ZONE_STATS = f"{HDFS_GOLD}/zone_stats"
HDFS_GOLD_HOURLY_DEMAND = f"{HDFS_GOLD}/hourly_demand"
HDFS_GOLD_OD_FLOWS = f"{HDFS_GOLD}/od_flows"
HDFS_GOLD_BOROUGH_DAILY = f"{HDFS_GOLD}/borough_daily"
HDFS_GOLD_TEMPORAL_PATTERNS = f"{HDFS_GOLD}/temporal_patterns"
HDFS_GOLD_PREDICTIONS = f"{HDFS_GOLD}/predictions"
HDFS_GOLD_CLUSTERS = f"{HDFS_GOLD}/clusters"
HDFS_GOLD_ANOMALIES = f"{HDFS_GOLD}/anomalies"
HDFS_GOLD_ML_MODELS = f"{HDFS_GOLD}/ml_models"

# ---------------------------------------------------------------------------
# Export paths  (JSON artifacts for the web app)
# ---------------------------------------------------------------------------
EXPORT_DIR = "../backend/data"

# ---------------------------------------------------------------------------
# Spark tuning
# ---------------------------------------------------------------------------
SPARK_APP_NAME = "NYC-Urban-Mobility"
SPARK_CONF = {
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
}

# ---------------------------------------------------------------------------
# Unified schema column names  (Yellow + Green + HVFHV mapped to common names)
# ---------------------------------------------------------------------------
UNIFIED_COLUMNS = [
    "taxi_type",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
]

# ---------------------------------------------------------------------------
# US Federal holidays (2023-2024) for feature engineering
# ---------------------------------------------------------------------------
US_HOLIDAYS = [
    "2023-01-02", "2023-01-16", "2023-02-20", "2023-05-29",
    "2023-06-19", "2023-07-04", "2023-09-04", "2023-10-09",
    "2023-11-10", "2023-11-23", "2023-12-25",
    "2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27",
    "2024-06-19", "2024-07-04", "2024-09-02", "2024-10-14",
    "2024-11-11", "2024-11-28", "2024-12-25",
]

# ---------------------------------------------------------------------------
# ML parameters
# ---------------------------------------------------------------------------
ML_TRAIN_YEAR = 2023
ML_TEST_YEAR = 2024
DEMAND_PREDICTION_TOP_ZONES = 20
KMEANS_K_RANGE = range(4, 10)
ANOMALY_ZSCORE_THRESHOLD = 3.0

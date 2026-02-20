"""
Shared utility functions for the Spark pipeline.
"""

from pyspark.sql import SparkSession
from config import SPARK_APP_NAME, SPARK_CONF


def get_spark(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """Create or retrieve a SparkSession with project-wide configuration."""
    builder = SparkSession.builder.appName(app_name)
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def log_df_info(df, name: str) -> None:
    """Print schema and row count for a DataFrame (debugging helper)."""
    print(f"\n{'='*60}")
    print(f"  DataFrame: {name}")
    print(f"  Rows:      {df.count():,}")
    print(f"  Columns:   {len(df.columns)}")
    print(f"{'='*60}")
    df.printSchema()


def write_partitioned_parquet(df, path: str, partition_cols: list, mode: str = "overwrite"):
    """Write DataFrame as partitioned Parquet with consistent settings."""
    (
        df.write
        .mode(mode)
        .partitionBy(*partition_cols)
        .parquet(path)
    )
    print(f"  ✓ Written to {path} (partitioned by {partition_cols})")

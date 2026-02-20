#!/usr/bin/env python3
"""
Step 5: Train ML models on enriched data → gold layer predictions.

Models:
  A. Demand Prediction (GBTRegressor) — hourly trip count per zone
  B. Zone Clustering (KMeans) — mobility profile classification
  C. Anomaly Detection — statistical z-score outlier flagging

Usage:
    spark-submit 05_ml_models.py
"""

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (
    RegressionEvaluator,
    ClusteringEvaluator,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from utils import get_spark, log_df_info
from config import (
    HDFS_SILVER_ENRICHED,
    HDFS_GOLD_ZONE_STATS,
    HDFS_GOLD_PREDICTIONS,
    HDFS_GOLD_CLUSTERS,
    HDFS_GOLD_ANOMALIES,
    HDFS_GOLD_ML_MODELS,
    ML_TRAIN_YEAR,
    ML_TEST_YEAR,
    DEMAND_PREDICTION_TOP_ZONES,
    KMEANS_K_RANGE,
    ANOMALY_ZSCORE_THRESHOLD,
)


# ===================================================================
# A. DEMAND PREDICTION  (GBTRegressor with CrossValidator)
# ===================================================================

def train_demand_model(spark):
    """
    Predict hourly trip count per zone using Gradient Boosted Trees.
    Train on 2023, evaluate on 2024.
    """
    print("\n" + "=" * 60)
    print("  MODEL A: Demand Prediction (GBTRegressor)")
    print("=" * 60)

    # Load zone-hour demand table (from feature engineering step)
    zone_hour_path = HDFS_SILVER_ENRICHED + "_zone_hour_demand"
    demand = spark.read.parquet(zone_hour_path)

    # Identify top N busiest zones
    top_zones = (
        demand
        .groupBy("pu_location_id")
        .agg(F.sum("trip_count").alias("total"))
        .orderBy(F.desc("total"))
        .limit(DEMAND_PREDICTION_TOP_ZONES)
        .select("pu_location_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    print(f"  Top {len(top_zones)} zones by volume: {top_zones}")

    demand = demand.filter(F.col("pu_location_id").isin(top_zones))

    # Add time features from datetime_hour
    demand = (
        demand
        .withColumn("hour", F.hour("datetime_hour"))
        .withColumn("dow", F.dayofweek("datetime_hour"))
        .withColumn("month", F.month("datetime_hour"))
        .withColumn("is_weekend",
                     F.when(F.dayofweek("datetime_hour").isin(1, 7), 1).otherwise(0))
        .withColumn("year", F.year("datetime_hour"))
    )

    # Drop rows with null lag features (first week of data)
    demand = demand.na.drop(subset=[
        "demand_lag_1h", "demand_lag_24h", "demand_lag_168h", "demand_rolling_7d_avg"
    ])

    # Feature columns
    feature_cols = [
        "pu_location_id", "hour", "dow", "month", "is_weekend",
        "demand_lag_1h", "demand_lag_24h", "demand_lag_168h",
        "demand_rolling_7d_avg",
    ]

    # Train / test split by year
    train = demand.filter(F.col("year") == ML_TRAIN_YEAR)
    test = demand.filter(F.col("year") == ML_TEST_YEAR)

    print(f"  Train rows: {train.count():,}  Test rows: {test.count():,}")

    # MLlib Pipeline
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withStd=True, withMean=True)
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="trip_count",
        predictionCol="predicted_demand",
        maxIter=50,
        maxDepth=5,
    )

    pipeline = Pipeline(stages=[assembler, scaler, gbt])

    # Hyperparameter tuning with CrossValidator
    param_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [4, 6, 8])
        .addGrid(gbt.maxIter, [30, 50])
        .build()
    )

    evaluator = RegressionEvaluator(
        labelCol="trip_count",
        predictionCol="predicted_demand",
        metricName="rmse",
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2,
    )

    print("  Training with 3-fold cross-validation...")
    cv_model = cv.fit(train)
    best_model = cv_model.bestModel

    # Evaluate on test set
    predictions = best_model.transform(test)
    rmse = evaluator.evaluate(predictions)
    mae_eval = RegressionEvaluator(
        labelCol="trip_count", predictionCol="predicted_demand", metricName="mae"
    )
    mae = mae_eval.evaluate(predictions)
    r2_eval = RegressionEvaluator(
        labelCol="trip_count", predictionCol="predicted_demand", metricName="r2"
    )
    r2 = r2_eval.evaluate(predictions)

    print(f"\n  ── Test Set Metrics ──")
    print(f"  RMSE: {rmse:.2f}")
    print(f"  MAE:  {mae:.2f}")
    print(f"  R²:   {r2:.4f}")

    # Save predictions
    output = predictions.select(
        "pu_location_id", "datetime_hour", "hour", "dow", "month",
        "trip_count", "predicted_demand",
    )
    output.write.mode("overwrite").parquet(HDFS_GOLD_PREDICTIONS)
    print(f"  ✓ Predictions written to {HDFS_GOLD_PREDICTIONS}")

    # Save model
    best_model.write().overwrite().save(HDFS_GOLD_ML_MODELS + "/demand_gbt")
    print(f"  ✓ Model saved to {HDFS_GOLD_ML_MODELS}/demand_gbt")

    return predictions


# ===================================================================
# B. ZONE CLUSTERING  (KMeans with Silhouette tuning)
# ===================================================================

def train_zone_clusters(spark):
    """
    Cluster zones by mobility profile using KMeans.
    Features: demand volume, peak patterns, fare levels, distance, tip behavior.
    """
    print("\n" + "=" * 60)
    print("  MODEL B: Zone Clustering (KMeans)")
    print("=" * 60)

    zone_stats = spark.read.parquet(HDFS_GOLD_ZONE_STATS)

    # Feature columns for clustering
    cluster_features = [
        "total_trips", "avg_fare", "avg_distance", "avg_duration_min",
        "avg_speed_mph", "avg_tip_pct", "peak_hour",
    ]

    # Drop zones with null features
    zone_data = zone_stats.na.drop(subset=cluster_features)
    print(f"  Zones with complete data: {zone_data.count()}")

    # Assemble and scale
    assembler = VectorAssembler(inputCols=cluster_features, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features",
                            withStd=True, withMean=True)

    prep_pipeline = Pipeline(stages=[assembler, scaler])
    prep_model = prep_pipeline.fit(zone_data)
    zone_scaled = prep_model.transform(zone_data)

    # Find optimal k via silhouette score
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        metricName="silhouette",
    )

    best_k, best_score = 5, -1
    print("\n  k | Silhouette Score")
    print("  ──┼─────────────────")
    for k in KMEANS_K_RANGE:
        kmeans = KMeans(featuresCol="features", k=k, seed=42, maxIter=50)
        model = kmeans.fit(zone_scaled)
        preds = model.transform(zone_scaled)
        score = evaluator.evaluate(preds)
        print(f"  {k} | {score:.4f}")
        if score > best_score:
            best_k, best_score = k, score

    print(f"\n  Best k={best_k} (silhouette={best_score:.4f})")

    # Train final model with best k
    final_kmeans = KMeans(featuresCol="features", k=best_k, seed=42, maxIter=100)
    final_model = final_kmeans.fit(zone_scaled)
    clustered = final_model.transform(zone_scaled)

    # Label clusters based on characteristics
    cluster_profiles = (
        clustered
        .groupBy("prediction")
        .agg(
            F.avg("total_trips").alias("avg_trips"),
            F.avg("avg_fare").alias("avg_fare"),
            F.avg("avg_distance").alias("avg_dist"),
            F.avg("avg_tip_pct").alias("avg_tip"),
        )
        .orderBy("prediction")
    )
    print("\n  Cluster profiles:")
    cluster_profiles.show()

    # Save cluster assignments
    output = clustered.select(
        "zone_id", "borough", "zone_name", "total_trips",
        "avg_fare", "avg_distance", "avg_duration_min", "avg_speed_mph",
        "avg_tip_pct", "peak_hour",
        F.col("prediction").alias("cluster_id"),
    )
    output.write.mode("overwrite").parquet(HDFS_GOLD_CLUSTERS)
    print(f"  ✓ Cluster assignments written to {HDFS_GOLD_CLUSTERS}")

    return output


# ===================================================================
# C. ANOMALY DETECTION  (Statistical z-score method)
# ===================================================================

def detect_anomalies(spark):
    """
    Flag zone-hours with abnormal demand using z-score method.
    Baseline: 2023 seasonal norms. Detect anomalies in 2024.
    """
    print("\n" + "=" * 60)
    print("  MODEL C: Anomaly Detection (Z-Score)")
    print("=" * 60)

    zone_hour_path = HDFS_SILVER_ENRICHED + "_zone_hour_demand"
    demand = spark.read.parquet(zone_hour_path)

    # Add time components
    demand = (
        demand
        .withColumn("hour", F.hour("datetime_hour"))
        .withColumn("dow", F.dayofweek("datetime_hour"))
        .withColumn("year", F.year("datetime_hour"))
    )

    # Compute baseline stats from 2023 (mean and stddev per zone × hour × dow)
    baseline = (
        demand
        .filter(F.col("year") == ML_TRAIN_YEAR)
        .groupBy("pu_location_id", "hour", "dow")
        .agg(
            F.avg("trip_count").alias("baseline_mean"),
            F.stddev("trip_count").alias("baseline_std"),
            F.count("*").alias("sample_count"),
        )
        .filter(F.col("sample_count") >= 4)  # need enough samples
    )

    # Join 2024 data with baselines
    test_data = demand.filter(F.col("year") == ML_TEST_YEAR)
    scored = test_data.join(
        baseline,
        on=["pu_location_id", "hour", "dow"],
        how="inner",
    )

    # Compute z-score
    scored = scored.withColumn(
        "z_score",
        F.when(
            F.col("baseline_std") > 0,
            (F.col("trip_count") - F.col("baseline_mean")) / F.col("baseline_std")
        ).otherwise(0)
    )

    # Flag anomalies
    scored = scored.withColumn(
        "is_anomaly",
        F.when(F.abs(F.col("z_score")) >= ANOMALY_ZSCORE_THRESHOLD, True).otherwise(False)
    )

    # Severity levels
    scored = scored.withColumn(
        "severity",
        F.when(F.abs(F.col("z_score")) >= 5, "critical")
        .when(F.abs(F.col("z_score")) >= 4, "high")
        .when(F.abs(F.col("z_score")) >= 3, "medium")
        .otherwise("normal")
    )

    # Direction: surge or drop
    scored = scored.withColumn(
        "direction",
        F.when(F.col("z_score") > 0, "surge").otherwise("drop")
    )

    anomalies = scored.filter(F.col("is_anomaly") == True)
    total_anomalies = anomalies.count()
    total_scored = scored.count()
    print(f"  Total scored zone-hours: {total_scored:,}")
    print(f"  Anomalies detected:      {total_anomalies:,} ({total_anomalies/max(total_scored,1)*100:.2f}%)")

    # Save anomalies
    output = anomalies.select(
        "pu_location_id", "datetime_hour", "hour", "dow",
        "trip_count", "baseline_mean", "baseline_std",
        "z_score", "severity", "direction",
    ).orderBy(F.desc(F.abs(F.col("z_score"))))

    output.write.mode("overwrite").parquet(HDFS_GOLD_ANOMALIES)
    print(f"  ✓ Anomalies written to {HDFS_GOLD_ANOMALIES}")

    # Print top anomalies
    print("\n  Top 10 anomalies:")
    output.show(10, truncate=False)

    return output


def main():
    spark = get_spark("NYC-Mobility-ML")

    train_demand_model(spark)
    train_zone_clusters(spark)
    detect_anomalies(spark)

    spark.stop()
    print("\n✓ All ML models complete.")


if __name__ == "__main__":
    main()

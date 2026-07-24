"""Micro-batch read-only prediction serving from approved Spark ML artifacts."""

import json
import os
from datetime import datetime, timezone
from urllib.request import urlopen

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from feature_contract import FEATURE_VERSION, HORIZONS_MINUTES
from model_registry import ModelRecord, approved_for
from signal_contract import guarded_signal
from train_baseline_models import FEATURE_COLUMNS

FEATURES_PATH = "s3a://crypto-lake/features_delta/market_features_v1"
PREDICTIONS_PATH = "s3a://crypto-lake/predictions_delta/read_only_signals_v1"


def spark_session():
    return (
        SparkSession.builder.appName("Read-Only-Prediction-Serving")
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_PASS"))
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def load_registry(url):
    """Load a JSON event list; no model is selected unless it is explicitly approved."""
    with urlopen(url, timeout=15) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError("registry manifest must contain a JSON list")
    return tuple(ModelRecord(**item) for item in payload)


def latest_features(features, drift_zscore_threshold):
    """Choose latest rows and flag extreme standardized-volume drift before scoring."""
    complete = F.lit(True)
    for name in FEATURE_COLUMNS:
        complete = complete & F.col(name).isNotNull()
    latest = features.filter(F.col("feature_version") == FEATURE_VERSION).withColumn(
        "feature_complete", complete
    ).withColumn(
        "drift_detected", F.abs(F.col("volume_zscore_60m")) >= F.lit(drift_zscore_threshold)
    ).withColumn(
        "rank", F.row_number().over(Window.partitionBy("symbol").orderBy(F.col("as_of_ts").desc()))
    )
    return latest.filter(F.col("rank") == 1).drop("rank")


def main():
    registry_url = os.environ.get("MODEL_REGISTRY_URL")
    if not registry_url:
        raise ValueError("MODEL_REGISTRY_URL is required and must reference the reviewed registry manifest")
    max_age = float(os.environ.get("PREDICTION_MAX_DATA_AGE_SECONDS", "180"))
    high_volatility = float(os.environ.get("PREDICTION_HIGH_VOLATILITY_PCT", "2.0"))
    min_edge = float(os.environ.get("LABEL_EDGE_THRESHOLD_PCT", "0.40"))
    drift_zscore_threshold = float(os.environ.get("PREDICTION_DRIFT_ZSCORE_THRESHOLD", "4.0"))
    if max_age <= 0 or high_volatility <= 0 or min_edge < 0 or drift_zscore_threshold <= 0:
        raise ValueError("prediction guardrail thresholds must be positive (minimum edge may be zero)")
    registry = load_registry(registry_url)
    spark = spark_session()
    try:
        features = latest_features(spark.read.format("delta").load(FEATURES_PATH), drift_zscore_threshold)
        now_epoch = datetime.now(timezone.utc).timestamp()
        for horizon in HORIZONS_MINUTES:
            champion = approved_for(registry, FEATURE_VERSION, horizon)
            if champion is None:
                print(f"No approved {horizon}m model; no predictions written.")
                continue
            predictions = PipelineModel.load(champion.model_path).transform(features)
            probability_up = F.col("probability")[1]
            probability_down = F.col("probability")[0]
            expected_return = F.greatest(probability_up, probability_down) * F.lit(champion.expected_return_pct or 0.0)
            decision = F.udf(
                lambda up, down, edge, age, complete, drift, volatility: guarded_signal(
                    up, down, edge, data_age_seconds=age, max_data_age_seconds=max_age,
                    feature_complete=complete, model_approved=True, drift_detected=drift,
                    volatility_pct=volatility, high_volatility_pct=high_volatility,
                    min_expected_edge_pct=min_edge,
                ),
                "struct<action:string,risk_tier:string,reason:string>",
            )
            output = predictions.withColumn("probability_up", probability_up).withColumn(
                "probability_down", probability_down
            ).withColumn("expected_return_pct", expected_return).withColumn(
                "data_age_seconds", F.lit(now_epoch) - F.col("as_of_ts").cast("double")
            ).withColumn("decision", decision(
                "probability_up", "probability_down", "expected_return_pct", "data_age_seconds",
                "feature_complete", "drift_detected", "volatility_15m_pct",
            )).select(
                "symbol", "as_of_ts", "feature_version", "probability_up", "probability_down",
                "expected_return_pct", "data_age_seconds", "feature_complete",
                "drift_detected", F.lit(horizon).alias("horizon_minutes"),
                F.lit(champion.model_id).alias("model_id"), F.lit(champion.model_path).alias("model_path"),
                F.col("decision.action").alias("action"), F.col("decision.risk_tier").alias("risk_tier"),
                F.col("decision.reason").alias("decision_reason"), F.current_timestamp().alias("served_at"),
            )
            output.write.format("delta").mode("append").save(PREDICTIONS_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

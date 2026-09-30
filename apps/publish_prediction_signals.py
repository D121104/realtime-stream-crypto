"""Publish read-only Delta prediction audit records to ClickHouse for Grafana."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PREDICTIONS_PATH = "s3a://crypto-lake/predictions_delta/read_only_signals_v1"
PREDICTIONS_TABLE = "crypto_prediction_signals"


def spark_session():
    return (
        SparkSession.builder.appName("Prediction-Signal-Publisher")
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_PASS"))
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def prediction_id_columns():
    """Return deterministic fields that identify one model decision without credentials."""
    return ["symbol", "as_of_ts", "horizon_minutes", "feature_version", "model_id"]


def main():
    jdbc_url = os.environ.get("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/default")
    jdbc_properties = {
        "user": os.environ.get("CLICKHOUSE_USER", "default"),
        "password": os.environ.get("CLICKHOUSE_PASS"),
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }
    spark = spark_session()
    try:
        source = spark.read.format("delta").load(PREDICTIONS_PATH)
        identifier = prediction_id_columns()
        published = source.withColumn(
            "prediction_id", F.sha2(F.concat_ws("|", *[F.col(name).cast("string") for name in identifier]), 256)
        ).dropDuplicates(["prediction_id"]).select(
            "prediction_id", "symbol", "as_of_ts", "horizon_minutes", "feature_version", "model_id",
            "probability_up", "probability_down", "expected_return_pct", "data_age_seconds",
            "feature_complete", "drift_detected", "action", "risk_tier", "decision_reason", "served_at",
        )
        if not published.isEmpty():
            published.write.jdbc(url=jdbc_url, table=PREDICTIONS_TABLE, mode="append", properties=jdbc_properties)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

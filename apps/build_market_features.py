"""Build a leak-free, versioned feature dataset from closed Bronze Binance klines."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from feature_contract import FEATURE_VERSION, HORIZONS_MINUTES

BRONZE_KLINES_PATH = "s3a://crypto-lake/bronze_delta/binance_klines_1m"
FEATURES_PATH = "s3a://crypto-lake/features_delta/market_features_v1"
TRAIN_END_DATE_ENV = "FEATURE_TRAIN_END_DATE"
VALIDATION_END_DATE_ENV = "FEATURE_VALIDATION_END_DATE"


def spark_session():
    minio_user = os.environ.get("MINIO_USER")
    minio_pass = os.environ.get("MINIO_PASS")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName("Market-Feature-Build")
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def feature_frame(klines, edge_threshold_pct):
    """Build historical features and labels without leaking future values into features."""
    ordered = Window.partitionBy("symbol").orderBy("open_time_ms")
    trailing_15 = ordered.rowsBetween(-14, 0)
    trailing_60 = ordered.rowsBetween(-59, 0)
    base = klines.filter(
        F.col("symbol").isNotNull()
        & F.col("open_time_ms").isNotNull()
        & (F.col("close_price") > 0)
        & (F.col("high_price") > 0)
        & (F.col("low_price") > 0)
        & (F.col("volume") >= 0)
    ).dropDuplicates(["symbol", "open_time_ms"])
    frame = (
        base.withColumn("as_of_ts", F.col("open_time"))
        .withColumn("previous_close", F.lag("close_price").over(ordered))
        .withColumn(
            "return_1m_pct",
            F.when(F.col("previous_close") > 0, ((F.col("close_price") / F.col("previous_close")) - 1) * 100),
        )
        .withColumn("return_15m_pct", ((F.col("close_price") / F.lag("close_price", 15).over(ordered)) - 1) * 100)
        .withColumn("return_60m_pct", ((F.col("close_price") / F.lag("close_price", 60).over(ordered)) - 1) * 100)
        .withColumn("volatility_15m_pct", F.stddev_samp("return_1m_pct").over(trailing_15))
        .withColumn("volume_mean_60m", F.avg("volume").over(trailing_60))
        .withColumn("volume_stddev_60m", F.stddev_samp("volume").over(trailing_60))
        .withColumn(
            "volume_zscore_60m",
            F.when(F.col("volume_stddev_60m") > 0, (F.col("volume") - F.col("volume_mean_60m")) / F.col("volume_stddev_60m")),
        )
        .withColumn("vwap_volume_15m", F.sum("volume").over(trailing_15))
        .withColumn(
            "vwap_15m",
            F.when(
                F.col("vwap_volume_15m") > 0,
                F.sum(F.col("close_price") * F.col("volume")).over(trailing_15) / F.col("vwap_volume_15m"),
            ),
        )
        .withColumn("vwap_deviation_15m_pct", ((F.col("close_price") / F.col("vwap_15m")) - 1) * 100)
        .withColumn("range_pct", ((F.col("high_price") - F.col("low_price")) / F.col("close_price")) * 100)
        .withColumn("trade_count_mean_15m", F.avg("trade_count").over(trailing_15))
        .withColumn("trade_count_zscore_15m", F.when(
            F.stddev_samp("trade_count").over(trailing_15) > 0,
            (F.col("trade_count") - F.avg("trade_count").over(trailing_15)) / F.stddev_samp("trade_count").over(trailing_15),
        ))
        .withColumn("minute_of_day_utc", F.hour("as_of_ts") * 60 + F.minute("as_of_ts"))
        .withColumn("feature_version", F.lit(FEATURE_VERSION))
        .withColumn("data_cutoff_ts", F.col("as_of_ts"))
    )
    for horizon in HORIZONS_MINUTES:
        future_close = F.lead("close_price", horizon).over(ordered)
        future_open_time_ms = F.lead("open_time_ms", horizon).over(ordered)
        has_exact_horizon = future_open_time_ms == F.col("open_time_ms") + F.lit(horizon * 60_000)
        label_return = F.when(has_exact_horizon, ((future_close / F.col("close_price")) - 1) * 100)
        frame = frame.withColumn(f"label_return_{horizon}m_pct", label_return).withColumn(
            f"label_{horizon}m",
            F.when(label_return.isNull(), F.lit(None).cast("string"))
            .when(label_return >= F.lit(edge_threshold_pct), F.lit("UP"))
            .when(label_return <= F.lit(-edge_threshold_pct), F.lit("DOWN"))
            .otherwise(F.lit("NEUTRAL")),
        )
    return frame.withColumn("event_date", F.to_date("as_of_ts"))


def main():
    edge_threshold_pct = float(os.environ.get("LABEL_EDGE_THRESHOLD_PCT", "0.40"))
    train_end_date = os.environ.get(TRAIN_END_DATE_ENV)
    validation_end_date = os.environ.get(VALIDATION_END_DATE_ENV)
    if edge_threshold_pct <= 0:
        raise ValueError("LABEL_EDGE_THRESHOLD_PCT must be positive")
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        klines = spark.read.format("delta").load(BRONZE_KLINES_PATH)
        features = feature_frame(klines, edge_threshold_pct)
        if train_end_date and validation_end_date:
            if train_end_date >= validation_end_date:
                raise ValueError(f"{TRAIN_END_DATE_ENV} must be before {VALIDATION_END_DATE_ENV}")
            features = features.withColumn(
                "split_id",
                F.when(F.to_date("as_of_ts") <= F.lit(train_end_date), F.lit("train"))
                .when(F.to_date("as_of_ts") <= F.lit(validation_end_date), F.lit("validation"))
                .otherwise(F.lit("holdout")),
            )
        else:
            features = features.withColumn("split_id", F.lit("unassigned"))
        features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
            "event_date", "symbol"
        ).save(FEATURES_PATH)
        print(f"Wrote {FEATURE_VERSION} market features to {FEATURES_PATH}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

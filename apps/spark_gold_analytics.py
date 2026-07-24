"""Gold layer: enrich Silver Delta aggregates and publish analytics plus price alerts."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

SILVER_PATH = "s3a://crypto-lake/silver_delta/crypto_trades_aggregated"
CHECKPOINT_PATH = "s3a://crypto-lake/checkpoints/gold_delta"
ANALYTICS_TABLE = "gold_crypto_analytics_v2"
ALERTS_TABLE = "crypto_price_alerts"

minio_user = os.environ.get("MINIO_USER")
minio_pass = os.environ.get("MINIO_PASS")
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
clickhouse_user = os.environ.get("CLICKHOUSE_USER", "default")
clickhouse_pass = os.environ.get("CLICKHOUSE_PASS")
clickhouse_url = os.environ.get("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/default")
alert_threshold_pct = float(os.environ.get("ALERT_PRICE_CHANGE_PCT", "1.0"))

spark = (
    SparkSession.builder.appName("Crypto-Gold-Analytics")
    .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.clickhouse:clickhouse-jdbc:0.6.5,"
        "org.apache.httpcomponents.client5:httpclient5:5.2.1",
    )
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", minio_user)
    .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

jdbc_options = {
    "url": clickhouse_url,
    "user": clickhouse_user,
    "password": clickhouse_pass,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def latest_prices_for(symbols):
    """Read one prior VWAP per symbol so price change survives micro-batch boundaries."""
    if not symbols:
        return spark.createDataFrame([], StructType([
            StructField("symbol", StringType(), False),
            StructField("previous_vwap", DoubleType(), True),
            StructField("previous_window_start", TimestampType(), True),
        ]))

    quoted_symbols = ",".join("'{}'".format(symbol.replace("'", "''")) for symbol in symbols)
    query = f"""(
        SELECT symbol, argMax(vwap, window_start) AS previous_vwap,
               max(window_start) AS previous_window_start
        FROM default.{ANALYTICS_TABLE} FINAL
        WHERE symbol IN ({quoted_symbols})
        GROUP BY symbol
    ) AS latest_prices"""
    return spark.read.jdbc(properties=jdbc_options, table=query)


def write_to_clickhouse(batch_df, batch_id):
    """Persist a completed Silver batch and emit threshold-based alerts."""
    if batch_df.isEmpty():
        return

    current = batch_df.filter(
        F.col("symbol").isNotNull()
        & F.col("window_start").isNotNull()
        & F.col("vwap").isNotNull()
        & (F.col("vwap") > 0)
    ).dropDuplicates(["symbol", "window_start"])
    if current.isEmpty():
        return

    symbols = [row.symbol for row in current.select("symbol").distinct().collect()]
    previous = latest_prices_for(symbols)
    analytics = (
        current.join(previous, "symbol", "left")
        .withColumn(
            "price_change_pct",
            F.when(
                F.col("previous_vwap").isNotNull() & (F.col("previous_vwap") > 0),
                ((F.col("vwap") - F.col("previous_vwap")) / F.col("previous_vwap")) * 100,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .select(
            "symbol", "window_start", "window_end", "avg_price", "low_price", "high_price",
            "total_volume", "trade_count", "vwap", "price_change_pct", "event_date", "batch_id",
        )
    )

    analytics.write.jdbc(properties=jdbc_options, table=ANALYTICS_TABLE, mode="append")

    alerts = (
        analytics.filter(F.abs(F.col("price_change_pct")) >= F.lit(alert_threshold_pct))
        .select(
            F.concat_ws(":", "symbol", F.date_format("window_start", "yyyyMMddHHmmss")).alias("alert_id"),
            "symbol",
            "window_start",
            "window_end",
            F.col("vwap").alias("observed_price"),
            "price_change_pct",
            F.lit(alert_threshold_pct).alias("threshold_pct"),
            F.when(F.col("price_change_pct") >= 0, F.lit("UP")).otherwise(F.lit("DOWN")).alias("direction"),
            F.when(F.abs(F.col("price_change_pct")) >= alert_threshold_pct * 2, F.lit("critical"))
            .otherwise(F.lit("warning")).alias("severity"),
        )
    )
    if not alerts.isEmpty():
        alerts.write.jdbc(properties=jdbc_options, table=ALERTS_TABLE, mode="append")


silver_stream = spark.readStream.format("delta").load(SILVER_PATH)
query = (
    silver_stream.writeStream.foreachBatch(write_to_clickhouse)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=os.environ.get("GOLD_TRIGGER", "60 seconds"))
    .start()
)

print("Gold analytics and price-alert streaming is running...")
query.awaitTermination()

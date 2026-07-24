"""Silver layer: one-minute, event-time aggregates from the validated Bronze Delta table."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_PATH = "s3a://crypto-lake/bronze_delta/crypto_trades"
SILVER_PATH = "s3a://crypto-lake/silver_delta/crypto_trades_aggregated"
CHECKPOINT_PATH = "s3a://crypto-lake/checkpoints/silver_delta"

minio_user = os.environ.get("MINIO_USER")
minio_pass = os.environ.get("MINIO_PASS")
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

spark = (
    SparkSession.builder.appName("Crypto-Silver-Aggregation")
    .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.cores.max", "4")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4")
    .config("spark.hadoop.fs.s3a.access.key", minio_user)
    .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

# Historical Bronze rows store event_timestamp as epoch milliseconds. Normalize
# that legacy BIGINT to a timestamp so watermarking works with both schemas.
if "event_time" not in bronze_stream.columns:
    bronze_stream = bronze_stream.withColumn(
        "event_time",
        (F.col("event_timestamp") / F.lit(1000)).cast("timestamp"),
    )

aggregated_trades = (
    bronze_stream.filter(
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("symbol").isNotNull()
        & (F.col("price") > 0)
        & (F.col("quantity") > 0)
    )
    .withWatermark("event_time", os.environ.get("SILVER_WATERMARK", "5 minutes"))
    .dropDuplicates(["event_id"])
    .groupBy(F.window("event_time", "1 minute"), F.col("symbol"))
    .agg(
        F.avg("price").alias("avg_price"),
        F.min("price").alias("low_price"),
        F.max("price").alias("high_price"),
        F.sum("quantity").alias("total_volume"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.count("event_id").cast("long").alias("trade_count"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "symbol",
        "avg_price",
        "low_price",
        "high_price",
        "total_volume",
        "vwap",
        "trade_count",
        F.to_date("window.start").alias("event_date"),
    )
)

query = (
    aggregated_trades.writeStream.format("delta")
    .outputMode("append")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("event_date", "symbol")
    .trigger(processingTime=os.environ.get("SILVER_TRIGGER", "60 seconds"))
    .start()
)

print("Silver aggregation streaming is running...")
query.awaitTermination()

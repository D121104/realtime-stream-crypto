"""Bronze ingestion: validate and persist Binance aggregate trades from Kafka."""

import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    from_unixtime,
    lit,
    to_date,
    when,
)
from pyspark.sql.types import LongType, StringType, StructField, StructType

BRONZE_PATH = "s3a://crypto-lake/bronze_delta/crypto_trades"
QUARANTINE_PATH = "s3a://crypto-lake/quarantine/crypto_trades"
CHECKPOINT_PATH = "s3a://crypto-lake/checkpoints/bronze_delta"

minio_user = os.environ.get("MINIO_USER")
minio_pass = os.environ.get("MINIO_PASS")
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

spark = (
    SparkSession.builder.appName("Crypto-Bronze-Streaming")
    .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
    .config("spark.cores.max", "2")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.access.key", minio_user)
    .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

trade_schema = StructType(
    [
        StructField(
            "metadata",
            StructType(
                [
                    StructField("event_id", StringType()),
                    StructField("event_timestamp", LongType()),
                    StructField("event_type", StringType()),
                    StructField("source", StringType()),
                    StructField("schema_version", StringType()),
                ]
            ),
        ),
        StructField(
            "payload",
            StructType(
                [
                    StructField("e", StringType()),
                    StructField("E", LongType()),
                    StructField("s", StringType()),
                    StructField("p", StringType()),
                    StructField("q", StringType()),
                    StructField("m", StringType()),
                ]
            ),
        ),
    ]
)

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_INTERNAL_HOST", "kafka:29092"))
    .option("subscribe", os.environ.get("KAFKA_TOPIC", "crypto-raw-data"))
    .option("startingOffsets", os.environ.get("KAFKA_STARTING_OFFSETS", "latest"))
    .load()
)

trades = (
    raw_stream.select(
        col("value").cast("string").alias("raw_message"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn("json_data", from_json(col("raw_message"), trade_schema))
    .select(
        "raw_message",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        col("json_data.metadata.event_id").alias("event_id"),
        col("json_data.metadata.event_timestamp").alias("event_timestamp_ms"),
        col("json_data.metadata.event_type").alias("event_type"),
        col("json_data.metadata.source").alias("source"),
        col("json_data.metadata.schema_version").alias("schema_version"),
        col("json_data.payload.s").alias("symbol"),
        col("json_data.payload.p").cast("double").alias("price"),
        col("json_data.payload.q").cast("double").alias("quantity"),
        col("json_data.payload.m").cast("boolean").alias("is_buyer_maker"),
    )
    .withColumn("event_time", from_unixtime(col("event_timestamp_ms") / 1000).cast("timestamp"))
    .withColumn("event_date", to_date(col("event_time")))
)

validation_reason = concat_ws(
    ",",
    when(col("event_id").isNull(), lit("missing_event_id")),
    when(col("symbol").isNull() | (col("symbol") == ""), lit("missing_symbol")),
    when(col("event_time").isNull(), lit("invalid_event_timestamp")),
    when(col("price").isNull() | (col("price") <= 0), lit("invalid_price")),
    when(col("quantity").isNull() | (col("quantity") <= 0), lit("invalid_quantity")),
)
validated_trades = trades.withColumn("validation_reason", validation_reason)


def write_bronze_and_quarantine(batch_df, batch_id):
    """Store invalid events separately and append de-duplicated valid events."""
    invalid_events = (
        batch_df.filter(col("validation_reason") != "")
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
    )
    if not invalid_events.rdd.isEmpty():
        invalid_events.write.format("delta").mode("append").partitionBy("event_date").save(QUARANTINE_PATH)

    valid_events = (
        batch_df.filter(col("validation_reason") == "")
        .drop("validation_reason", "raw_message")
        .dropDuplicates(["event_id"])
    )
    if valid_events.rdd.isEmpty():
        return

    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        (
            DeltaTable.forPath(spark, BRONZE_PATH)
            .alias("target")
            .merge(valid_events.alias("source"), "target.event_id = source.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        valid_events.write.format("delta").mode("append").partitionBy("event_date", "symbol").save(BRONZE_PATH)


query = (
    validated_trades.writeStream.foreachBatch(write_bronze_and_quarantine)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime=os.environ.get("BRONZE_TRIGGER", "30 seconds"))
    .start()
)

print("Bronze streaming with validation and quarantine is running...")
query.awaitTermination()

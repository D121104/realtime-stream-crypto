from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType
from pyspark.sql.functions import col, to_date, from_unixtime, from_json, window, expr
import os

minio_user = os.environ.get("MINIO_USER")
minio_pass = os.environ.get("MINIO_PASS")
minio_endpoint = os.environ.get("MINIO_ENDPOINT")

spark = SparkSession.builder \
    .appName("Crypto-Silver-Aggregation") \
    .master("spark://spark-master:7077") \
    .config("spark.cores.max", "2") \
    .config("spark.sql.caseSensitive", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", minio_user) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_pass) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("metadata", StructType([
        StructField("event_id", StringType()),
        StructField("event_timestamp", LongType()),
        StructField("event_type", StringType()),
        StructField("source", StringType()),
        StructField("schema_version", StringType())
    ]), True),
    StructField("payload", StructType([
        StructField("e", StringType(), True), # Loại sự kiện
        StructField("E", LongType(), True),   # Event time từ Binance
        StructField("s", StringType(), True), # Ký hiệu coin (e.g., BTCUSDT)
        StructField("p", StringType(), True), # Giá (Dạng string từ Binance)
        StructField("q", StringType(), True), # Khối lượng (Dạng string)
        StructField("m", StringType(), True)  # Is buyer maker
    ]), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto-raw-data") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df \
    .withColumn("value_string", col('value').cast('string')) \
    .withColumn("json_data", from_json(col('value_string'), schema)) \
    .withColumn("event_time", from_unixtime(col('json_data.metadata.event_timestamp')/1000).cast("timestamp")) \
    .withColumn("symbol", col("json_data.payload.s")) \
    .withColumn("price", col("json_data.payload.p").cast("double")) \
    .withColumn("quantity", col("json_data.payload.q").cast("double")) \
    .withColumn("event_type", col("json_data.payload.e")) \
    .withColumn("event_timestamp", col("json_data.metadata.event_timestamp"))

aggregate_df = parsed_df \
    .withWatermark("event_time", "5 minutes" ) \
    .groupBy(
        window(col("event_time"), "1 minutes"),
        col("symbol")
    )\
    .agg(
        expr("avg(price)").alias("avg_price"),
        expr("min(price)").alias("low_price"),
        expr("max(price)").alias("high_price"),
        expr("sum(quantity)").alias("total_volume"),
        expr("sum(price * quantity) / sum(quantity)").alias("vwap"),
        expr("count(1)").alias("trade_count")
    )\
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("avg_price"),
        col("low_price"),
        col("high_price"),
        col("total_volume"),
        col("vwap"),
        col("trade_count"),
        to_date(col("window_start")).alias("event_date")
    )

query = aggregate_df.writeStream\
    .format("parquet")\
    .outputMode("append")\
    .option("path", "s3a://crypto-lake/silver/crypto_trades_aggregated")\
    .option("checkpointLocation", "s3a://crypto-lake/checkpoints/crypto_trades_aggregated")\
    .partitionBy("event_date")\
    .trigger(processingTime="60 seconds")\
    .start()


print("Spark Streaming is running...")
query.awaitTermination()
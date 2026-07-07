from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType
from pyspark.sql.functions import from_json, col, from_unixtime, to_date
import os

minio_user = os.environ.get("MINIO_USER")
minio_pass = os.environ.get("MINIO_PASS")

spark = SparkSession.builder \
    .appName('Crypto Spark Streaming') \
    .master('spark://spark-master:7077') \
    .config("spark.cores.max", "2") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4') \
    .config('spark.hadoop.fs.s3a.access.key', minio_user) \
    .config('spark.hadoop.fs.s3a.secret.key', minio_pass) \
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
    .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

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

kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "crypto-raw-data") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_stream_df \
    .withColumn("value_string", col('value').cast('string')) \
    .withColumn("json_data", from_json(col('value_string'), schema))

final_df = parsed_df.select(
    col('json_data.metadata.event_id').alias('event_id'),
    col('json_data.metadata.event_timestamp').alias('event_timestamp'),
    col('json_data.metadata.event_type').alias('event_type'),
    col('json_data.metadata.source').alias('source'),
    col('json_data.metadata.schema_version').alias('schema_version'),
    col('json_data.payload.s').alias('symbol'),
    col('json_data.payload.p').alias('price'),
    col('json_data.payload.q').alias('quantity'),
    to_date(from_unixtime(col('json_data.metadata.event_timestamp')/1000)).alias('event_date')
)

query = final_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://crypto-lake/bronze/crypto_trades") \
    .option("checkpointLocation", "s3a://crypto-lake/checkpoints/crypto_trades") \
    .partitionBy("event_date", "symbol") \
    .start()

print("Spark Streaming is running...")
query.awaitTermination()
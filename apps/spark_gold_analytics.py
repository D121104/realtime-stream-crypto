import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, DateType
from pyspark.sql import functions as F

minio_user = os.environ.get("MINIO_USER")
minio_pass = os.environ.get("MINIO_PASS")
minio_endpoint = os.environ.get("MINIO_ENDPOINT")

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Spark-Gold-Analytics") \
    .config("spark.hadoop.fs.s3a.endpoint",  minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_user) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_pass) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 2. Định nghĩa Schema đọc từ tầng Silver
silver_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("trade_count", LongType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("event_date", DateType(), True)
])

# 3. Đọc Stream Parquet từ tầng Silver (MinIO)
silver_stream = spark.readStream \
    .schema(silver_schema) \
    .parquet("s3a://crypto-lake/silver/crypto_trades_aggregated")

gold_df = silver_stream.filter(F.col("symbol").isNotNull() & F.col("avg_price").isNotNull())

# 4. Hàm ghi dữ liệu vào ClickHouse sử dụng ForeachBatch (Chuẩn JDBC Spark)
def write_to_clickhouse(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/default") \
        .option("dbtable", "gold_crypto_analytics") \
        .option("user", os.environ.get("CLICKHOUSE_USER")) \
        .option("password", os.environ.get("CLICKHOUSE_PASS")) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()

# 5. Kích hoạt luồng Stream sang Gold
query = gold_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "s3a://crypto-lake/checkpoints/gold/") \
    .start()

print("Spark Gold Analytics Streaming is running...")
query.awaitTermination()
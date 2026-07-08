import os
from pyspark.sql import SparkSession

minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
minio_user = os.environ.get("MINIO_USER")
minio_pass = os.environ.get("MINIO_PASS")

spark = SparkSession.builder \
    .appName("Parquet-To-Delta-Migration") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_user) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_pass) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

old_silver_path = f"s3a://crypto-lake/silver/crypto_trades_aggregated"
new_silver_path = f"s3a://crypto-lake/silver_delta/crypto_trades_aggregated"

old_bronze_path = f"s3a://crypto-lake/bronze/crypto_trades"
new_bronze_path = f"s3a://crypto-lake/bronze_delta/crypto_trades"

try:
    old_silver_df = spark.read.parquet(old_silver_path)
    
    old_silver_df.write \
        .format("delta") \
        .partitionBy("event_date") \
        .mode("overwrite") \
        .save(new_silver_path)

    old_bronze_df = spark.read.parquet(old_bronze_path)
    
    old_bronze_df.write \
        .format("delta") \
        .partitionBy("event_date") \
        .mode("overwrite") \
        .save(new_bronze_path)

except Exception as e:
    print(f"Migrate fail: {str(e)}")

finally:
    spark.stop()
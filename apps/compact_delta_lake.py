import os
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def main():
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_user = os.environ.get("MINIO_USER")
    minio_pass = os.environ.get("MINIO_PASS")

    if not minio_user or not minio_pass:
        print("Error: Missing MINIO_USER or MINIO_PASS environment variables!")
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("Delta-Lake-Compaction-Daily") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    delta_tables = {
        "Bronze Layer": f"s3a://crypto-lake/bronze_delta/crypto_trades",
        "Silver Layer": f"s3a://crypto-lake/silver_delta/crypto_trades_aggregated"
    }

    for name, path in delta_tables.items():
        print(f"\nProcessing [{name}] at: {path}")
        try:
            print(f"-> Compacting {yesterday}")
            spark.sql(f"OPTIMIZE delta.`{path}` WHERE event_date = '{yesterday}'")
            print(f"-> OPTIMIZE success!")

            print(f"-> VACUUM")
            spark.sql(f"VACUUM delta.`{path}` RETAIN 24 HOURS")
            print(f"-> VACUUM success!")

        except Exception as e:
            print(f"-> Failed to process {name}: {str(e)}")

    spark.stop()
    print(f"\n==================================================")
    print(f"Process complete at: {datetime.now()}")
    print(f"==================================================")

if __name__ == "__main__":
    main()
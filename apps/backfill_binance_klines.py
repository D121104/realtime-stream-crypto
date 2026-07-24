"""Idempotently backfill closed Binance Spot one-minute klines into Bronze Delta."""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from market_symbols import configured_symbols, parse_symbols

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
BRONZE_KLINES_PATH = "s3a://crypto-lake/bronze_delta/binance_klines_1m"
INTERVAL_MS = 60_000
MAX_LIMIT = 1000
def kline_schema():
    """Build the Spark schema only when the backfill runs in a Spark runtime."""
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("open_time_ms", LongType(), False),
            StructField("close_time_ms", LongType(), False),
            StructField("open_price", DoubleType(), False),
            StructField("high_price", DoubleType(), False),
            StructField("low_price", DoubleType(), False),
            StructField("close_price", DoubleType(), False),
            StructField("volume", DoubleType(), False),
            StructField("quote_volume", DoubleType(), False),
            StructField("trade_count", LongType(), False),
            StructField("taker_buy_base_volume", DoubleType(), False),
            StructField("taker_buy_quote_volume", DoubleType(), False),
        ]
    )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default=os.getenv("CRYPTO_SYMBOLS", ",".join(configured_symbols())))
    parser.add_argument("--start-ms", type=int, default=None, help="UTC epoch milliseconds, inclusive")
    parser.add_argument("--end-ms", type=int, default=None, help="UTC epoch milliseconds, exclusive")
    parser.add_argument("--days", type=int, default=int(os.getenv("KLINE_BACKFILL_DAYS", "365")))
    parser.add_argument("--request-delay-seconds", type=float, default=float(os.getenv("KLINE_REQUEST_DELAY_SECONDS", "0.15")))
    parser.add_argument("--max-retries", type=int, default=int(os.getenv("KLINE_MAX_RETRIES", "5")))
    return parser.parse_args()


def closed_minute_cutoff_ms(now_ms=None):
    """Return the start of the currently open minute; only earlier klines are closed."""
    current = int(time.time() * 1000) if now_ms is None else int(now_ms)
    return (current // INTERVAL_MS) * INTERVAL_MS


def request_klines(symbol, start_ms, end_ms, opener=urlopen, max_retries=5):
    """Fetch at most one Binance page and return decoded JSON rows."""
    parameters = urlencode(
        {"symbol": symbol.upper(), "interval": "1m", "startTime": start_ms, "endTime": end_ms, "limit": MAX_LIMIT}
    )
    url = f"{BINANCE_KLINES_URL}?{parameters}"
    for attempt in range(max_retries):
        try:
            with opener(url, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as error:
            if attempt == max_retries - 1:
                raise RuntimeError(f"could not fetch klines for {symbol} starting {start_ms}") from error
            time.sleep(min(2 ** attempt, 30))
    raise AssertionError("unreachable")


def normalize_kline(symbol, row, closed_cutoff_ms):
    """Convert one Binance Kline array to a typed row; skip an open candle."""
    if len(row) < 11:
        raise ValueError("Binance kline row has fewer than 11 fields")
    open_time_ms, close_time_ms = int(row[0]), int(row[6])
    if close_time_ms >= closed_cutoff_ms:
        return None
    return (
        symbol.lower(), open_time_ms, close_time_ms, float(row[1]), float(row[2]), float(row[3]),
        float(row[4]), float(row[5]), float(row[7]), int(row[8]), float(row[9]), float(row[10]),
    )


def fetch_symbol_klines(
    symbol, start_ms, end_ms, request_delay_seconds=0.0, request_fn=request_klines,
    cutoff_ms=None, max_retries=5,
):
    """Yield normalized, closed rows and advance safely even after sparse pages."""
    cursor = start_ms
    closed_cutoff = closed_minute_cutoff_ms() if cutoff_ms is None else cutoff_ms
    while cursor < end_ms:
        page_end = min(end_ms, cursor + MAX_LIMIT * INTERVAL_MS)
        page = request_fn(symbol, cursor, page_end, max_retries=max_retries)
        if not page:
            cursor = page_end
        else:
            for row in page:
                normalized = normalize_kline(symbol, row, closed_cutoff)
                if normalized is not None:
                    yield normalized
            cursor = max(page_end, int(page[-1][0]) + INTERVAL_MS)
        if request_delay_seconds:
            time.sleep(request_delay_seconds)


def spark_session():
    from pyspark.sql import SparkSession

    minio_user = os.environ.get("MINIO_USER")
    minio_pass = os.environ.get("MINIO_PASS")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName("Binance-Kline-Backfill")
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


def write_klines(spark, rows):
    """Merge a non-empty Kline batch, keyed by Binance's immutable open time."""
    if not rows:
        return 0
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    frame = spark.createDataFrame(rows, kline_schema()).withColumn(
        "open_time", F.to_timestamp(F.from_unixtime(F.col("open_time_ms") / F.lit(1000)))
    ).withColumn("event_date", F.to_date("open_time")).withColumn("source", F.lit("binance_rest_klines"))
    if DeltaTable.isDeltaTable(spark, BRONZE_KLINES_PATH):
        DeltaTable.forPath(spark, BRONZE_KLINES_PATH).alias("target").merge(
            frame.alias("source"), "target.symbol = source.symbol AND target.open_time_ms = source.open_time_ms"
        ).whenNotMatchedInsertAll().execute()
    else:
        frame.write.format("delta").mode("append").partitionBy("event_date", "symbol").save(BRONZE_KLINES_PATH)
    return len(rows)


def main():
    args = parse_args()
    symbols = parse_symbols(args.symbols, required_count=None)
    end_ms = min(args.end_ms or closed_minute_cutoff_ms(), closed_minute_cutoff_ms())
    start_ms = args.start_ms or int((datetime.now(timezone.utc) - timedelta(days=args.days)).timestamp() * 1000)
    if start_ms >= end_ms:
        raise ValueError("start time must be before the latest closed minute")

    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        for symbol in symbols:
            rows = list(
                fetch_symbol_klines(
                    symbol, start_ms, end_ms, args.request_delay_seconds,
                    cutoff_ms=end_ms, max_retries=args.max_retries,
                )
            )
            count = write_klines(spark, rows)
            print(f"Backfilled {count} closed one-minute klines for {symbol}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

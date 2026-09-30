param(
  [int]$Days = 365,
  [string]$Symbols = $env:CRYPTO_SYMBOLS
)

if (-not $Symbols) {
  $Symbols = "btcusdt,ethusdt,solusdt,bnbusdt,xrpusdt,dogeusdt,adausdt,trxusdt,avaxusdt,linkusdt"
}

$packages = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

docker exec -it `
  -e MINIO_ENDPOINT="$env:MINIO_ENDPOINT" `
  -e MINIO_USER="$env:MINIO_USER" `
  -e MINIO_PASS="$env:MINIO_PASS" `
  -e SPARK_MASTER_URL="$env:SPARK_MASTER_URL" `
  spark-master /opt/spark/bin/spark-submit `
  --driver-memory 2g `
  --packages $packages `
  /opt/spark/apps/backfill_binance_klines.py `
  --symbols $Symbols `
  --days $Days

# Run read-only Spark prediction scoring manually after a human approves registry entries.
param(
  [string]$RegistryUrl = $env:MODEL_REGISTRY_URL
)

if ([string]::IsNullOrWhiteSpace($RegistryUrl)) {
  throw "MODEL_REGISTRY_URL is required; serving refuses to choose an unreviewed model."
}

$env:MODEL_REGISTRY_URL = $RegistryUrl

docker compose exec -T `
  -e MINIO_ENDPOINT="$env:MINIO_ENDPOINT" `
  -e MINIO_USER="$env:MINIO_USER" `
  -e MINIO_PASS="$env:MINIO_PASS" `
  -e SPARK_MASTER_URL="$env:SPARK_MASTER_URL" `
  -e MODEL_REGISTRY_URL="$env:MODEL_REGISTRY_URL" `
  -e LABEL_EDGE_THRESHOLD_PCT="$env:LABEL_EDGE_THRESHOLD_PCT" `
  -e PREDICTION_MAX_DATA_AGE_SECONDS="$env:PREDICTION_MAX_DATA_AGE_SECONDS" `
  -e PREDICTION_HIGH_VOLATILITY_PCT="$env:PREDICTION_HIGH_VOLATILITY_PCT" `
  -e PREDICTION_DRIFT_ZSCORE_THRESHOLD="$env:PREDICTION_DRIFT_ZSCORE_THRESHOLD" `
  spark-master /opt/spark/bin/spark-submit `
  --driver-memory 2g `
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 `
  /opt/spark/apps/serve_predictions.py

$packages = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

docker exec -it `
  -e MINIO_ENDPOINT="$env:MINIO_ENDPOINT" `
  -e MINIO_USER="$env:MINIO_USER" `
  -e MINIO_PASS="$env:MINIO_PASS" `
  -e SPARK_MASTER_URL="$env:SPARK_MASTER_URL" `
  -e LABEL_EDGE_THRESHOLD_PCT="$env:LABEL_EDGE_THRESHOLD_PCT" `
  -e FEATURE_TRAIN_END_DATE="$env:FEATURE_TRAIN_END_DATE" `
  -e FEATURE_VALIDATION_END_DATE="$env:FEATURE_VALIDATION_END_DATE" `
  spark-master /opt/spark/bin/spark-submit `
  --driver-memory 3g `
  --packages $packages `
  /opt/spark/apps/build_market_features.py

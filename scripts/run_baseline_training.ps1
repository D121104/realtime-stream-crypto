$packages = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

docker exec -it `
  -e MINIO_ENDPOINT="$env:MINIO_ENDPOINT" `
  -e MINIO_USER="$env:MINIO_USER" `
  -e MINIO_PASS="$env:MINIO_PASS" `
  -e SPARK_MASTER_URL="$env:SPARK_MASTER_URL" `
  -e MODEL_CONFIDENCE_THRESHOLD="$env:MODEL_CONFIDENCE_THRESHOLD" `
  spark-master /opt/spark/bin/spark-submit `
  --driver-memory 4g `
  --packages $packages `
  /opt/spark/apps/train_baseline_models.py

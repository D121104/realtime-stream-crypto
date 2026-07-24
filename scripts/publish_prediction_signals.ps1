# Publish Delta prediction audit records to ClickHouse for the private Grafana dashboard.
docker compose exec -T `
  -e MINIO_ENDPOINT="$env:MINIO_ENDPOINT" `
  -e MINIO_USER="$env:MINIO_USER" `
  -e MINIO_PASS="$env:MINIO_PASS" `
  -e SPARK_MASTER_URL="$env:SPARK_MASTER_URL" `
  -e CLICKHOUSE_USER="$env:CLICKHOUSE_USER" `
  -e CLICKHOUSE_PASS="$env:CLICKHOUSE_PASS" `
  spark-master /opt/spark/bin/spark-submit `
  --driver-memory 2g `
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1 `
  /opt/spark/apps/publish_prediction_signals.py

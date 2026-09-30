$packages = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.clickhouse:clickhouse-jdbc:0.6.5,org.apache.httpcomponents.client5:httpclient5:5.2.1"

docker exec -it spark-master /opt/spark/bin/spark-submit `
  --driver-memory 4g `
  --packages $packages `
  /opt/spark/apps/spark_gold_analytics.py

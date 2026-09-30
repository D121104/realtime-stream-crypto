$packages = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

docker exec -it spark-master /opt/spark/bin/spark-submit `
  --driver-memory 2g `
  --packages $packages `
  /opt/spark/apps/spark_silver_aggregation.py

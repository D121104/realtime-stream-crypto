"""Train chronological Spark ML logistic-regression baselines and cost-aware backtests."""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from backtest_contract import SIDE_COST_PCT
from feature_contract import FEATURE_VERSION, HORIZONS_MINUTES

FEATURES_PATH = "s3a://crypto-lake/features_delta/market_features_v1"
MODEL_OUTPUT_PATH = "s3a://crypto-lake/models/candidates/logistic_regression"
REPORTS_PATH = "s3a://crypto-lake/model_reports/logistic_regression"
FEATURE_COLUMNS = [
    "return_1m_pct", "return_15m_pct", "return_60m_pct", "volatility_15m_pct",
    "volume_zscore_60m", "vwap_deviation_15m_pct", "range_pct",
    "trade_count_zscore_15m", "minute_of_day_utc",
]


def spark_session():
    minio_user = os.environ.get("MINIO_USER")
    minio_pass = os.environ.get("MINIO_PASS")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName("Market-Baseline-Training")
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def required_columns(horizon):
    return FEATURE_COLUMNS + [
        "symbol", "as_of_ts", "split_id", "feature_version", f"label_{horizon}m", f"label_return_{horizon}m_pct"
    ]


def prepared_features(features, horizon):
    """Select fully-known rows only; no holdout row can influence the fitted pipeline."""
    label_col = f"label_{horizon}m"
    return features.select(*required_columns(horizon)).filter(
        (F.col("feature_version") == FEATURE_VERSION)
        & (F.col("split_id").isin("train", "validation", "holdout"))
        & (F.col(label_col).isin("UP", "DOWN"))
        & F.col(f"label_return_{horizon}m_pct").isNotNull()
        & F.col("as_of_ts").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("return_1m_pct").isNotNull()
        & F.col("return_15m_pct").isNotNull()
        & F.col("return_60m_pct").isNotNull()
        & F.col("volatility_15m_pct").isNotNull()
        & F.col("volume_zscore_60m").isNotNull()
        & F.col("vwap_deviation_15m_pct").isNotNull()
        & F.col("range_pct").isNotNull()
        & F.col("trade_count_zscore_15m").isNotNull()
    )


def pipeline():
    symbol_indexer = StringIndexer(inputCol="symbol", outputCol="symbol_index", handleInvalid="skip")
    label_indexer = StringIndexer(
        inputCol="label", outputCol="label_index", handleInvalid="error", stringOrderType="alphabetAsc"
    )
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS + ["symbol_index"], outputCol="features")
    classifier = LogisticRegression(featuresCol="features", labelCol="label_index", probabilityCol="probability", maxIter=100)
    return Pipeline(stages=[symbol_indexer, label_indexer, assembler, classifier])


def backtest(predictions, horizon, confidence_threshold):
    """Create cost-adjusted reports; each selected trade pays both side costs."""
    label_return = F.col(f"label_return_{horizon}m_pct")
    probability_up = F.col("probability")[1]
    probability_down = F.col("probability")[0]
    confidence = F.greatest(probability_up, probability_down)
    direction = F.when(probability_up > probability_down, F.lit("UP")).otherwise(F.lit("DOWN"))
    gross_return = F.when(direction == "UP", label_return).otherwise(-label_return)
    selected = predictions.withColumn("probability_up", probability_up).withColumn(
        "probability_down", probability_down
    ).withColumn("confidence", confidence).withColumn("direction", direction).filter(
        F.col("confidence") >= F.lit(confidence_threshold)
    ).withColumn("gross_return_pct", gross_return).withColumn(
        "net_return_pct", gross_return - F.lit(2 * SIDE_COST_PCT)
    )
    report = selected.groupBy("symbol").agg(
        F.count("*").alias("trade_count"),
        F.sum("net_return_pct").alias("net_return_pct"),
        F.avg(F.when(F.col("net_return_pct") > 0, F.lit(1.0)).otherwise(F.lit(0.0))).alias("win_rate"),
        F.avg("confidence").alias("avg_confidence"),
    ).withColumn("horizon_minutes", F.lit(horizon)).withColumn("confidence_threshold", F.lit(confidence_threshold)).withColumn(
        "feature_version", F.lit(FEATURE_VERSION)
    )
    return selected, report


def main():
    confidence_threshold = float(os.environ.get("MODEL_CONFIDENCE_THRESHOLD", "0.55"))
    if not 0.5 < confidence_threshold < 1:
        raise ValueError("MODEL_CONFIDENCE_THRESHOLD must be between 0.5 and 1")
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        features = spark.read.format("delta").load(FEATURES_PATH)
        if features.filter(F.col("split_id") == "unassigned").limit(1).count():
            raise ValueError("feature dataset has unassigned splits; set chronological split dates before training")
        for horizon in HORIZONS_MINUTES:
            label_col = f"label_{horizon}m"
            dataset = prepared_features(features, horizon).withColumnRenamed(label_col, "label")
            train = dataset.filter(F.col("split_id") == "train")
            validation = dataset.filter(F.col("split_id") == "validation")
            holdout = dataset.filter(F.col("split_id") == "holdout")
            if not train.limit(1).count() or not validation.limit(1).count() or not holdout.limit(1).count():
                raise ValueError(f"horizon {horizon}m requires non-empty train, validation, and holdout splits")
            model = pipeline().fit(train)
            model_path = f"{MODEL_OUTPUT_PATH}/{FEATURE_VERSION}/{horizon}m"
            model.write().overwrite().save(model_path)
            evaluator = MulticlassClassificationEvaluator(labelCol="label_index", predictionCol="prediction", metricName="accuracy")
            validation_accuracy = evaluator.evaluate(model.transform(validation))
            holdout_predictions = model.transform(holdout)
            holdout_accuracy = evaluator.evaluate(holdout_predictions)
            _, report = backtest(holdout_predictions, horizon, confidence_threshold)
            report = report.withColumn("validation_accuracy", F.lit(validation_accuracy)).withColumn(
                "holdout_accuracy", F.lit(holdout_accuracy)
            ).withColumn("model_path", F.lit(model_path))
            report.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
                f"{REPORTS_PATH}/{FEATURE_VERSION}/{horizon}m"
            )
            print(f"Trained {horizon}m baseline: validation_accuracy={validation_accuracy:.4f}, holdout_accuracy={holdout_accuracy:.4f}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

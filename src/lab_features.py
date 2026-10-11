"""Spark schema and shared session features for the controlled traffic lab."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


FLOW_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("experiment_id", StringType(), True),
        StructField("lab_mode", StringType(), True),
        StructField("record_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source_ip", StringType(), True),
        StructField("source_port", IntegerType(), True),
        StructField("destination_port", IntegerType(), True),
        StructField("proto", StringType(), True),
        StructField("service", StringType(), True),
        StructField("dur", DoubleType(), True),
        StructField("sbytes", LongType(), True),
        StructField("dbytes", LongType(), True),
    ]
)

SESSION_FEATURES = (
    "flow_count",
    "destination_port_count",
    "elapsed_seconds",
    "flow_rate",
    "mean_sbytes",
    "mean_dbytes",
    "mean_duration",
    "http_ratio",
)
WINDOW_FEATURES = (
    "flow_count",
    "elapsed_seconds",
    "flow_rate",
    "mean_sbytes",
    "mean_dbytes",
    "mean_duration",
    "http_ratio",
    "port_scan_ratio",
)

WINDOW_SECONDS = 10
WATERMARK_SECONDS = 30


def aggregate_windows(flows: DataFrame) -> DataFrame:
    """The same observed-source features for batch training and streaming state."""
    events = flows.withColumn("event_time", F.to_timestamp("timestamp")).where(
        F.col("event_time").isNotNull() & F.col("source_ip").isNotNull()
        & F.col("experiment_id").isNotNull()
    )
    if flows.isStreaming:
        events = events.withWatermark("event_time", f"{WATERMARK_SECONDS} seconds")
    grouped = events.groupBy(
        "experiment_id", "source_ip", F.window("event_time", f"{WINDOW_SECONDS} seconds")
    ).agg(
        F.first("actor_id", ignorenulls=True).alias("actor_id"),
        F.count("*").cast("double").alias("flow_count"),
        F.avg(F.when(F.col("destination_port") != 8080, 1.0).otherwise(0.0)).alias("port_scan_ratio"),
        F.avg("sbytes").cast("double").alias("mean_sbytes"),
        F.avg("dbytes").cast("double").alias("mean_dbytes"),
        F.avg("dur").cast("double").alias("mean_duration"),
        F.avg(F.when(F.col("service") == "http", 1.0).otherwise(0.0)).alias("http_ratio"),
    )
    return grouped.withColumn("elapsed_seconds", F.lit(float(WINDOW_SECONDS))).withColumn(
        "flow_rate", F.col("flow_count") / F.lit(float(WINDOW_SECONDS))
    ).select(
        "experiment_id", "source_ip", "actor_id", F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"), *WINDOW_FEATURES,
    )


def aggregate_sessions(flows: DataFrame) -> DataFrame:
    """Build one unlabeled Spark feature row per generated traffic run."""
    events = (
        flows.where(
            F.col("run_id").isNotNull()
            & (F.col("record_type") == "flow")
        )
        .withColumn("_event_time", F.to_timestamp("timestamp"))
    )
    grouped = events.groupBy("run_id").agg(
        F.count("*").cast("double").alias("flow_count"),
        F.countDistinct("destination_port").cast("double").alias("destination_port_count"),
        F.min("_event_time").alias("_first_seen"),
        F.max("_event_time").alias("_last_seen"),
        F.avg("sbytes").cast("double").alias("mean_sbytes"),
        F.avg("dbytes").cast("double").alias("mean_dbytes"),
        F.avg("dur").cast("double").alias("mean_duration"),
        F.avg(F.when(F.col("service") == "http", 1.0).otherwise(0.0)).alias("http_ratio"),
    )
    return (
        grouped.withColumn(
            "elapsed_seconds",
            F.greatest(
                F.col("_last_seen").cast("double") - F.col("_first_seen").cast("double"),
                F.lit(0.001),
            ),
        )
        .withColumn(
            "flow_rate",
            F.col("flow_count") / F.greatest(F.col("elapsed_seconds"), F.lit(1.0)),
        )
        .select("run_id", *SESSION_FEATURES)
    )

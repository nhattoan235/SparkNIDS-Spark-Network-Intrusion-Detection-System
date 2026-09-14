"""Phase-10 file-source Structured Streaming inference with checkpoint recovery."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from src.finalize_model import directory_sha256
from src.predict_batch import OUTPUT_NAME_PATTERN, build_prediction_output
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.unsw_nb15_schema import UNSW_NB15_SCHEMA


BATCH_SUMMARY_SCHEMA = StructType(
    [
        StructField("stream_batch_id", LongType(), False),
        StructField("query_run_id", StringType(), False),
        StructField("processed_at", StringType(), False),
        StructField("input_rows", LongType(), False),
        StructField("alert_rows", LongType(), False),
        StructField("normal_rows", LongType(), False),
        StructField("alert_rate", DoubleType(), False),
        StructField("max_attack_probability", DoubleType(), False),
        StructField("mean_attack_probability", DoubleType(), False),
        StructField("source_file_count", LongType(), False),
    ]
)


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _resolve_project_path(value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _paths_overlap(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def validate_stream_paths(
    input_dir: str | Path,
    predictions_dir: str | Path,
    summaries_dir: str | Path,
    checkpoint_dir: str | Path,
) -> tuple[Path, Path, Path, Path]:
    """Resolve stream paths and reject source/sink/checkpoint overlap."""
    paths = tuple(
        _resolve_project_path(path)
        for path in (input_dir, predictions_dir, summaries_dir, checkpoint_dir)
    )
    names = ("input", "predictions", "summaries", "checkpoint")
    for left in range(len(paths)):
        for right in range(left + 1, len(paths)):
            if _paths_overlap(paths[left], paths[right]):
                raise ValueError(f"Streaming {names[left]} and {names[right]} paths must not overlap.")
    return paths


def build_streaming_source(
    spark: SparkSession,
    input_dir: str | Path,
    *,
    max_files_per_trigger: int,
) -> DataFrame:
    """Create a Parquet file stream with the explicit UNSW-NB15 schema."""
    if max_files_per_trigger < 1:
        raise ValueError("max_files_per_trigger must be at least 1")
    resolved_input = _resolve_project_path(input_dir)
    resolved_input.mkdir(parents=True, exist_ok=True)
    return (
        spark.readStream.schema(UNSW_NB15_SCHEMA)
        .option("maxFilesPerTrigger", int(max_files_per_trigger))
        .parquet(str(resolved_input))
        .withColumn("source_file", F.element_at(F.split(F.input_file_name(), r"[/\\]"), -1))
    )


def summarize_micro_batch(
    output: DataFrame,
    *,
    stream_batch_id: int,
    query_run_id: str,
    processed_at: str,
) -> dict[str, Any]:
    """Collect one bounded aggregate row describing a scored micro-batch."""
    row = output.agg(
        F.count("*").alias("input_rows"),
        F.sum(F.when(F.col("prediction") == 1, 1).otherwise(0)).alias("alert_rows"),
        F.max("attack_probability").alias("max_attack_probability"),
        F.avg("attack_probability").alias("mean_attack_probability"),
        F.countDistinct("source_file").alias("source_file_count"),
    ).first()
    input_rows = int(row["input_rows"])
    alert_rows = int(row["alert_rows"] or 0)
    return {
        "stream_batch_id": int(stream_batch_id),
        "query_run_id": query_run_id,
        "processed_at": processed_at,
        "input_rows": input_rows,
        "alert_rows": alert_rows,
        "normal_rows": input_rows - alert_rows,
        "alert_rate": alert_rows / input_rows if input_rows else 0.0,
        "max_attack_probability": float(row["max_attack_probability"] or 0.0),
        "mean_attack_probability": float(row["mean_attack_probability"] or 0.0),
        "source_file_count": int(row["source_file_count"]),
    }


def _load_locked_model(config: dict[str, Any]) -> tuple[PipelineModel, dict[str, Any], str]:
    final_config = config["final_model"]
    model_path = _resolve_project_path(final_config["model_path"])
    metadata_path = _resolve_project_path(final_config["metadata_path"])
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    model_sha256 = directory_sha256(model_path)
    if model_sha256 != metadata["pipeline_sha256"]:
        raise ValueError("Final PipelineModel checksum does not match locked metadata.")
    model = PipelineModel.load(str(model_path))
    stage_types = [stage.__class__.__name__ for stage in model.stages]
    if stage_types != metadata["pipeline_stage_types"]:
        raise ValueError("Reloaded PipelineModel stages differ from locked metadata.")
    return model, metadata, model_sha256


def _compact_progress(progress: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "batch_id": int(item["batchId"]),
            "input_rows": int(item["numInputRows"]),
            "duration_ms": {key: int(value) for key, value in item.get("durationMs", {}).items()},
        }
        for item in progress
    ]


def run_available_now(
    spark: SparkSession,
    config: dict[str, Any],
    *,
    input_dir: str | Path,
    output_root: str | Path,
    checkpoint_dir: str | Path,
    query_name: str,
) -> dict[str, Any]:
    """Process all currently available source files, then terminate."""
    if not OUTPUT_NAME_PATTERN.fullmatch(query_name):
        raise ValueError("query_name contains unsupported characters")
    output_root = _resolve_project_path(output_root)
    predictions_dir = output_root / "predictions"
    summaries_dir = output_root / "batch_summaries"
    input_dir, predictions_dir, summaries_dir, checkpoint_dir = validate_stream_paths(
        input_dir,
        predictions_dir,
        summaries_dir,
        checkpoint_dir,
    )
    for path in (predictions_dir, summaries_dir, checkpoint_dir):
        path.mkdir(parents=True, exist_ok=True)

    model, metadata, model_sha256 = _load_locked_model(config)
    threshold = float(metadata["decision_threshold"])
    max_files = int(config["streaming"]["max_files_per_trigger"])
    output_partitions = int(config["streaming"]["output_partitions"])
    source = build_streaming_source(spark, input_dir, max_files_per_trigger=max_files)
    query_run_id = f"phase10-{uuid.uuid4().hex[:12]}"

    def process_batch(raw_batch: DataFrame, batch_id: int) -> None:
        processed_at = _now()
        transformed = model.transform(raw_batch)
        _, scalar_output = build_prediction_output(
            transformed,
            threshold=threshold,
            run_id=query_run_id,
            scored_at=processed_at,
            model_name=metadata["model_name"],
            model_sha256=model_sha256,
        )
        enriched = (
            scalar_output.withColumn("stream_batch_id", F.lit(int(batch_id)).cast("long"))
            .withColumn("processed_at", F.lit(processed_at))
            .cache()
        )
        summary = summarize_micro_batch(
            enriched,
            stream_batch_id=int(batch_id),
            query_run_id=query_run_id,
            processed_at=processed_at,
        )
        batch_name = f"batch-{int(batch_id):020d}"
        enriched.coalesce(output_partitions).write.mode("overwrite").parquet(
            str(predictions_dir / batch_name)
        )
        spark.createDataFrame([summary], BATCH_SUMMARY_SCHEMA).coalesce(1).write.mode("overwrite").parquet(
            str(summaries_dir / batch_name)
        )
        enriched.unpersist()

    started = time.perf_counter()
    query = (
        source.writeStream.queryName(query_name)
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(checkpoint_dir))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    elapsed = time.perf_counter() - started
    return {
        "query_id": str(query.id),
        "query_run_id": str(query.runId),
        "output_run_id": query_run_id,
        "active_after_termination": query.isActive,
        "elapsed_seconds": elapsed,
        "progress": _compact_progress(query.recentProgress),
    }


def publish_demo_batch(
    spark: SparkSession,
    config: dict[str, Any],
    *,
    input_dir: Path,
    batch_index: int,
    rows_per_batch: int,
) -> int:
    """Write then atomically move one deterministic sample Parquet file."""
    sample_path = PROJECT_ROOT / config["paths"]["samples"] / "unsw_nb15_sample_csv"
    source = spark.read.option("header", True).schema(UNSW_NB15_SCHEMA).csv(str(sample_path))
    ranked = source.withColumn(
        "_demo_row_number",
        F.row_number().over(Window.orderBy("id", "label")),
    )
    start = batch_index * rows_per_batch
    batch = ranked.filter(
        (F.col("_demo_row_number") > start)
        & (F.col("_demo_row_number") <= start + rows_per_batch)
    ).drop("_demo_row_number")
    row_count = batch.count()
    if row_count != rows_per_batch:
        raise ValueError(
            f"Demo sample cannot provide batch {batch_index}: expected {rows_per_batch}, got {row_count}."
        )

    input_dir.mkdir(parents=True, exist_ok=True)
    destination = input_dir / f"batch-{batch_index:05d}.parquet"
    if destination.exists():
        raise FileExistsError(f"Demo batch already exists: {destination}")
    staging = input_dir.parent / f".{input_dir.name}-staging-{batch_index}-{uuid.uuid4().hex[:8]}"
    try:
        batch.coalesce(1).write.mode("errorifexists").parquet(str(staging))
        part_files = list(staging.glob("part-*.parquet"))
        if len(part_files) != 1:
            raise ValueError(f"Expected exactly one staged Parquet file, found {len(part_files)}.")
        part_files[0].replace(destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return row_count


def _read_recursive_parquet(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.option("recursiveFileLookup", True).parquet(str(path))


def run_phase10_demo(
    config_path: str | Path | dict[str, Any] | None = None,
    *,
    run_name: str | None = None,
    rows_per_batch: int | None = None,
    input_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Run two arrivals, restart from checkpoint, then process a third arrival."""
    config = config_path if isinstance(config_path, dict) else load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    streaming = config["streaming"]
    resolved_name = run_name or f"phase10-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    if not OUTPUT_NAME_PATTERN.fullmatch(resolved_name):
        raise ValueError("run_name must contain only letters, digits, dot, underscore or hyphen")
    batch_rows = int(rows_per_batch or streaming["demo_rows_per_batch"])
    initial_batch_count = int(streaming["initial_batch_count"])
    restart_batch_count = int(streaming["restart_batch_count"])
    if batch_rows < 1 or initial_batch_count < 1 or restart_batch_count < 1:
        raise ValueError("Demo row and batch counts must be positive")

    input_dir = _resolve_project_path(input_root or streaming["input_root"]) / resolved_name
    resolved_output_root = _resolve_project_path(output_root or streaming["output_root"]) / resolved_name
    checkpoint_dir = resolved_output_root / "checkpoint"
    if input_dir.exists() or resolved_output_root.exists():
        raise FileExistsError(f"Streaming demo run already exists: {resolved_name}")

    published_rows: list[int] = []
    first_spark = create_spark_session(config, app_name=f"unsw-nb15-streaming-first-{resolved_name}")
    try:
        for batch_index in range(initial_batch_count):
            published_rows.append(
                publish_demo_batch(
                    first_spark,
                    config,
                    input_dir=input_dir,
                    batch_index=batch_index,
                    rows_per_batch=batch_rows,
                )
            )
        first_query = run_available_now(
            first_spark,
            config,
            input_dir=input_dir,
            output_root=resolved_output_root,
            checkpoint_dir=checkpoint_dir,
            query_name=f"{resolved_name}-query",
        )
        first_predictions = _read_recursive_parquet(first_spark, resolved_output_root / "predictions").count()
        first_summaries = _read_recursive_parquet(first_spark, resolved_output_root / "batch_summaries").count()
        first_application_id = first_spark.sparkContext.applicationId
    finally:
        first_spark.stop()

    second_spark = create_spark_session(config, app_name=f"unsw-nb15-streaming-restart-{resolved_name}")
    try:
        for batch_index in range(initial_batch_count, initial_batch_count + restart_batch_count):
            published_rows.append(
                publish_demo_batch(
                    second_spark,
                    config,
                    input_dir=input_dir,
                    batch_index=batch_index,
                    rows_per_batch=batch_rows,
                )
            )
        second_query = run_available_now(
            second_spark,
            config,
            input_dir=input_dir,
            output_root=resolved_output_root,
            checkpoint_dir=checkpoint_dir,
            query_name=f"{resolved_name}-query",
        )
        predictions = _read_recursive_parquet(second_spark, resolved_output_root / "predictions").cache()
        summaries = _read_recursive_parquet(second_spark, resolved_output_root / "batch_summaries").cache()
        final_prediction_rows = predictions.count()
        distinct_ids = predictions.select("id").distinct().count()
        summary_rows = summaries.count()
        batch_summaries = [row.asDict(recursive=True) for row in summaries.orderBy("stream_batch_id").collect()]
        second_application_id = second_spark.sparkContext.applicationId
        predictions.unpersist()
        summaries.unpersist()
    finally:
        second_spark.stop()

    expected_batches = initial_batch_count + restart_batch_count
    expected_rows = expected_batches * batch_rows
    batch_ids = [int(item["stream_batch_id"]) for item in batch_summaries]
    restart_checks = {
        "same_persistent_query_id": first_query["query_id"] == second_query["query_id"],
        "different_query_run_id": first_query["query_run_id"] != second_query["query_run_id"],
        "first_output_preserved": first_predictions == initial_batch_count * batch_rows,
        "only_new_file_processed_after_restart": final_prediction_rows - first_predictions
        == restart_batch_count * batch_rows,
        "no_duplicate_ids": distinct_ids == final_prediction_rows,
        "consecutive_batch_ids": batch_ids == list(range(expected_batches)),
    }
    if (
        first_summaries != initial_batch_count
        or summary_rows != expected_batches
        or final_prediction_rows != expected_rows
        or not all(restart_checks.values())
    ):
        raise ValueError("Structured Streaming checkpoint/restart verification failed.")

    report = {
        "phase": 10,
        "status": "ok",
        "generated_at": _now(),
        "mode": "file-source micro-batch simulation; not live packet capture",
        "run_name": resolved_name,
        "input": {
            "path": str(input_dir),
            "format": "parquet",
            "explicit_schema": UNSW_NB15_SCHEMA.simpleString(),
            "published_files": expected_batches,
            "published_rows_by_file": published_rows,
            "max_files_per_trigger": int(streaming["max_files_per_trigger"]),
        },
        "model": {
            "path": str(_resolve_project_path(config["final_model"]["model_path"])),
            "name": config["final_model"]["name"],
            "decision_threshold": float(config["final_model"]["decision_threshold"]),
            "loaded_without_training": True,
        },
        "first_run": {
            **first_query,
            "spark_application_id": first_application_id,
            "prediction_rows": first_predictions,
            "summary_rows": first_summaries,
        },
        "restart_run": {
            **second_query,
            "spark_application_id": second_application_id,
            "final_prediction_rows": final_prediction_rows,
            "summary_rows": summary_rows,
        },
        "checkpoint": {
            "path": str(checkpoint_dir),
            "restart_checks": restart_checks,
        },
        "batch_summaries": batch_summaries,
        "outputs": {
            "root": str(resolved_output_root),
            "predictions": str(resolved_output_root / "predictions"),
            "batch_summaries": str(resolved_output_root / "batch_summaries"),
            "prediction_rows": final_prediction_rows,
            "distinct_ids": distinct_ids,
        },
        "delivery_semantics": {
            "foreach_batch": "at-least-once by Spark contract",
            "idempotence": "each deterministic batch_id overwrites only its own output directory",
        },
    }
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    run_report = resolved_output_root / "report.json"
    run_report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    metrics_path = PROJECT_ROOT / config["paths"]["outputs"] / "metrics" / "phase10_streaming.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Phase-10 Structured Streaming file-source demo.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    parser.add_argument("--run-name", default=None, help="Optional unique output/checkpoint name.")
    parser.add_argument("--rows-per-batch", type=int, default=None, help="Override the small demo batch size.")
    parser.add_argument("--input-dir", type=Path, default=None, help="Run once against this custom file-source directory.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Custom prediction/summary output root.")
    parser.add_argument("--checkpoint-dir", type=Path, default=None, help="Persistent checkpoint for custom mode.")
    parser.add_argument("--query-name", default="unsw-nb15-streaming", help="Stable query name for custom mode.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    custom_paths = (arguments.input_dir, arguments.output_dir, arguments.checkpoint_dir)
    if any(path is not None for path in custom_paths):
        if not all(path is not None for path in custom_paths):
            raise ValueError("--input-dir, --output-dir and --checkpoint-dir must be provided together")
        custom_config = load_config(arguments.config or PROJECT_ROOT / "configs" / "default.yaml")
        custom_spark = create_spark_session(custom_config, app_name=f"unsw-nb15-streaming-{arguments.query_name}")
        try:
            custom_report = run_available_now(
                custom_spark,
                custom_config,
                input_dir=arguments.input_dir,
                output_root=arguments.output_dir,
                checkpoint_dir=arguments.checkpoint_dir,
                query_name=arguments.query_name,
            )
            print(json.dumps(custom_report, indent=2, ensure_ascii=False))
        finally:
            custom_spark.stop()
    else:
        run_phase10_demo(
            arguments.config,
            run_name=arguments.run_name,
            rows_per_batch=arguments.rows_per_batch,
        )

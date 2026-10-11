"""Prepare verified UNSW data and measure a representative Spark query at real sizes."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.benchmark import representative_query, result_signature, timing_summary, track_query_execution
from src.download_data import download_dataset
from src.ingest import run_ingestion
from src.preprocess import run_preprocessing
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


def atomic_json(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{time.time_ns()}.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def prepare_dataset():
    config = load_config()
    raw = PROJECT_ROOT / config["paths"]["raw"] / config["dataset"]["files"]["train"]["filename"]
    bronze = PROJECT_ROOT / config["paths"]["bronze"] / "train"
    silver = PROJECT_ROOT / config["paths"]["silver"] / "train"
    if not raw.is_file():
        atomic_json(PROJECT_ROOT / "outputs/live_lab/scale.json", {"status": "downloading", "updated_at": time.time()})
        download_dataset(config)
    if not any(bronze.glob("*.parquet")):
        atomic_json(PROJECT_ROOT / "outputs/live_lab/scale.json", {"status": "bronze", "updated_at": time.time()})
        run_ingestion()
    if not any(silver.glob("*.parquet")):
        atomic_json(PROJECT_ROOT / "outputs/live_lab/scale.json", {"status": "silver", "updated_at": time.time()})
        run_preprocessing()


def run_scale() -> dict:
    config = load_config()
    parquet_path = PROJECT_ROOT / config["paths"]["silver"] / "train"
    source = PROJECT_ROOT / "outputs/live_lab/scale.json"
    if not any(parquet_path.glob("*.parquet")):
        raise FileNotFoundError("No Silver Parquet found. Prepare the verified UNSW train split first.")
    atomic_json(source, {"status": "benchmarking", "updated_at": time.time()})
    spark = create_spark_session(app_name="live-lab-real-data-scale")
    cached = None
    try:
        cached = spark.read.parquet(str(parquet_path)).select("label", "proto", "sbytes", "dbytes").cache()
        full_rows = cached.count()
        sizes = sorted({min(full_rows, target) for target in (5_000, 35_000, full_rows) if min(full_rows, target) > 0})
        results = []
        for row_count in sizes:
            input_frame = cached.limit(row_count)
            expected_signature = None
            partition_results = []
            partitions = (1,) if row_count < full_rows else (1, 2, 4)
            for requested in partitions:
                frame = input_frame.repartition(requested).cache()
                materialization_seconds = time.perf_counter()
                measured_rows = frame.count()
                materialization_seconds = time.perf_counter() - materialization_seconds
                representative_query(frame).collect()
                durations, task_counts, signatures = [], [], set()
                for _ in range(3):
                    aggregate, elapsed, evidence = track_query_execution(
                        spark, representative_query(frame), description=f"Scale demo {row_count} rows / {requested} partitions"
                    )
                    durations.append(elapsed)
                    task_counts.append(evidence["total_tasks_across_unique_stages"])
                    signatures.add(result_signature(aggregate))
                frame.unpersist(blocking=True)
                if len(signatures) != 1 or measured_rows != row_count:
                    raise ValueError(f"Unstable aggregate result at {row_count} rows/{requested} partitions")
                signature = next(iter(signatures))
                if expected_signature is None:
                    expected_signature = signature
                elif signature != expected_signature:
                    raise ValueError(f"Partition count changed the aggregate at {row_count} rows")
                partition_results.append({"requested_partitions": requested, "actual_partitions": requested,
                    "task_count": max(task_counts), "cache_materialization_seconds": materialization_seconds,
                    "query_timing": timing_summary(durations), "result_sha256": signature})
            results.append({"input_rows": row_count, "partition_results": partition_results})
        report = {"status": "ok", "data": "UNSW-NB15 verified Silver train", "total_train_rows": full_rows,
            "input_parquet_bytes": sum(path.stat().st_size for path in parquet_path.rglob("*.parquet")),
            "query": "select label, proto, sbytes, dbytes -> group by label/proto -> count and sum bytes",
            "warmup_runs_per_configuration": 1, "measured_runs_per_configuration": 3,
            "local_master": spark.sparkContext.master, "spark_version": spark.version,
            "windows": results, "notes": "Exact same aggregate and verified SHA-256 at every size/partition count. Local[2] on one machine; this demonstrates input growth and Spark task partitioning, not cluster speedup. Cached Silver input; query timing excludes data preparation.",
            "created_at": datetime.now(timezone.utc).isoformat()}
        atomic_json(PROJECT_ROOT / "outputs/benchmarks/live_lab_scale.json", report)
        atomic_json(source, report)
        print(json.dumps(report, ensure_ascii=False), flush=True)
        return report
    except Exception as error:
        atomic_json(source, {"status": "failed", "error": str(error), "updated_at": time.time()})
        raise
    finally:
        if cached is not None:
            cached.unpersist(blocking=True)
        spark.stop()


def main():
    try:
        prepare_dataset()
        run_scale()
    except Exception as error:
        atomic_json(PROJECT_ROOT / "outputs/live_lab/scale.json", {"status": "failed", "error": str(error), "updated_at": time.time()})
        raise


if __name__ == "__main__":
    main()

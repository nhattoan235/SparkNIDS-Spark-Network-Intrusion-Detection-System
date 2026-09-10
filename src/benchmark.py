"""Controlled local Spark benchmarks and execution evidence for Phase 8."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import math
import os
import platform
import shutil
import statistics
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.validate_data import read_unsw_csv


def representative_query(dataframe: DataFrame) -> DataFrame:
    """Exact-integer aggregate shared by every controlled comparison."""
    return (
        dataframe.filter(F.col("label").isin(0, 1))
        .groupBy("label", "proto")
        .agg(
            F.count("*").alias("flow_count"),
            F.sum("sbytes").alias("source_bytes"),
            F.sum("dbytes").alias("destination_bytes"),
        )
        .orderBy("label", "proto")
    )


def result_signature(rows: list[Row]) -> str:
    payload = [row.asDict(recursive=True) for row in rows]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def timing_summary(durations: list[float]) -> dict[str, Any]:
    if not durations:
        raise ValueError("At least one duration is required.")
    return {
        "runs": len(durations),
        "seconds": durations,
        "median_seconds": statistics.median(durations),
        "mean_seconds": statistics.fmean(durations),
        "min_seconds": min(durations),
        "max_seconds": max(durations),
    }


def _timed_collect(query: DataFrame) -> tuple[list[Row], float]:
    started = time.perf_counter()
    rows = query.collect()
    return rows, time.perf_counter() - started


def _directory_data_size(path: Path, pattern: str) -> int:
    return sum(item.stat().st_size for item in path.glob(pattern) if item.is_file())


def _physical_memory_bytes() -> int | None:
    if os.name != "nt":
        return None

    class MemoryStatus(ctypes.Structure):
        _fields_ = [
            ("length", ctypes.c_ulong),
            ("memory_load", ctypes.c_ulong),
            ("total_physical", ctypes.c_ulonglong),
            ("available_physical", ctypes.c_ulonglong),
            ("total_page_file", ctypes.c_ulonglong),
            ("available_page_file", ctypes.c_ulonglong),
            ("total_virtual", ctypes.c_ulonglong),
            ("available_virtual", ctypes.c_ulonglong),
            ("available_extended_virtual", ctypes.c_ulonglong),
        ]

    status = MemoryStatus()
    status.length = ctypes.sizeof(status)
    return int(status.total_physical) if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)) else None


def _tracked_collect(spark: Any, query: DataFrame) -> tuple[list[Row], float, dict[str, Any]]:
    group_id = f"phase8-evidence-{uuid.uuid4().hex[:10]}"
    context = spark.sparkContext
    tracker = context.statusTracker()
    context.setJobGroup(group_id, "Phase 8 representative groupBy/orderBy evidence")
    rows, duration = _timed_collect(query)
    job_ids = sorted(int(value) for value in tracker.getJobIdsForGroup(group_id))
    jobs = []
    unique_stage_ids: set[int] = set()
    for job_id in job_ids:
        info = tracker.getJobInfo(job_id)
        if info is None:
            continue
        stage_ids = [int(value) for value in info.stageIds]
        unique_stage_ids.update(stage_ids)
        jobs.append({"job_id": job_id, "status": str(info.status), "stage_ids": stage_ids})
    stages = []
    for stage_id in sorted(unique_stage_ids):
        info = tracker.getStageInfo(stage_id)
        if info is None:
            continue
        stages.append(
            {
                "stage_id": stage_id,
                "name": str(info.name),
                "num_tasks": int(info.numTasks),
                "completed_tasks": int(info.numCompletedTasks),
                "failed_tasks": int(info.numFailedTasks),
            }
        )
    context.setLocalProperty("spark.jobGroup.id", None)
    return rows, duration, {
        "job_group_id": group_id,
        "job_count": len(jobs),
        "jobs": jobs,
        "unique_stage_count": len(stages),
        "stages": stages,
        "total_tasks_across_unique_stages": sum(item["num_tasks"] for item in stages),
        "completed_tasks_across_unique_stages": sum(item["completed_tasks"] for item in stages),
        "failed_tasks_across_unique_stages": sum(item["failed_tasks"] for item in stages),
        "stages_with_completed_tasks": sum(1 for item in stages if item["completed_tasks"]),
    }


def _format_benchmark(spark: Any, *, csv_path: Path, parquet_path: Path, warmups: int, runs: int) -> dict[str, Any]:
    loaders: dict[str, Callable[[], DataFrame]] = {
        "csv": lambda: read_unsw_csv(spark, csv_path),
        "parquet": lambda: spark.read.parquet(str(parquet_path)),
    }
    for _ in range(warmups):
        for name in ("csv", "parquet"):
            representative_query(loaders[name]()).collect()

    measurements: dict[str, list[float]] = {"csv": [], "parquet": []}
    signatures: dict[str, set[str]] = {"csv": set(), "parquet": set()}
    aggregate_rows: dict[str, int] = {}
    input_partitions: dict[str, int] = {}
    for run_index in range(runs):
        order = ("csv", "parquet") if run_index % 2 == 0 else ("parquet", "csv")
        for name in order:
            frame = loaders[name]()
            input_partitions[name] = frame.rdd.getNumPartitions()
            rows, duration = _timed_collect(representative_query(frame))
            measurements[name].append(duration)
            signatures[name].add(result_signature(rows))
            aggregate_rows[name] = sum(int(row["flow_count"]) for row in rows)

    csv_summary = timing_summary(measurements["csv"])
    parquet_summary = timing_summary(measurements["parquet"])
    csv_signature = next(iter(signatures["csv"]))
    parquet_signature = next(iter(signatures["parquet"]))
    if len(signatures["csv"]) != 1 or len(signatures["parquet"]) != 1 or csv_signature != parquet_signature:
        raise ValueError("CSV and Parquet benchmark results are not identical.")
    return {
        "method": "alternating order after equal warmups; read + filter + groupBy + aggregates + orderBy + collect small aggregate",
        "warmup_runs_per_format": warmups,
        "measured_runs_per_format": runs,
        "csv": {
            "path": str(csv_path),
            "size_bytes": csv_path.stat().st_size,
            "input_partitions": input_partitions["csv"],
            "source_rows_verified_by_aggregate": aggregate_rows["csv"],
            "result_sha256": csv_signature,
            "timing": csv_summary,
        },
        "parquet": {
            "path": str(parquet_path),
            "size_bytes": _directory_data_size(parquet_path, "*.parquet"),
            "input_partitions": input_partitions["parquet"],
            "source_rows_verified_by_aggregate": aggregate_rows["parquet"],
            "result_sha256": parquet_signature,
            "timing": parquet_summary,
        },
        "parquet_speedup_over_csv_median": csv_summary["median_seconds"] / parquet_summary["median_seconds"],
        "identical_results": True,
    }


def _cache_benchmark(spark: Any, *, parquet_path: Path, runs: int) -> dict[str, Any]:
    uncached = spark.read.parquet(str(parquet_path))
    representative_query(uncached).collect()
    uncached_durations = []
    uncached_signature = None
    for _ in range(runs):
        rows, duration = _timed_collect(representative_query(uncached))
        uncached_durations.append(duration)
        uncached_signature = result_signature(rows)

    cached = spark.read.parquet(str(parquet_path)).cache()
    materialize_started = time.perf_counter()
    materialized_rows = cached.count()
    materialize_seconds = time.perf_counter() - materialize_started
    cached_durations = []
    cached_signature = None
    for _ in range(runs):
        rows, duration = _timed_collect(representative_query(cached))
        cached_durations.append(duration)
        cached_signature = result_signature(rows)
    storage_level = str(cached.storageLevel)
    was_cached = cached.is_cached
    cached.unpersist(blocking=True)

    uncached_summary = timing_summary(uncached_durations)
    cached_summary = timing_summary(cached_durations)
    if uncached_signature != cached_signature:
        raise ValueError("Cached and uncached query results differ.")
    uncached_total = sum(uncached_durations)
    cached_reuse_total = sum(cached_durations)
    cached_end_to_end_total = materialize_seconds + cached_reuse_total
    median_saving = uncached_summary["median_seconds"] - cached_summary["median_seconds"]
    return {
        "method": "same Parquet DataFrame and aggregate reused; cache materialization reported separately",
        "measured_reuse_runs": runs,
        "materialized_rows": materialized_rows,
        "cache_storage_level": storage_level,
        "cache_was_active": was_cached,
        "cache_materialization_seconds": materialize_seconds,
        "without_cache": uncached_summary,
        "with_cache": cached_summary,
        "cache_speedup_median": uncached_summary["median_seconds"] / cached_summary["median_seconds"],
        "three_reuses_without_cache_total_seconds": uncached_total,
        "three_reuses_with_cache_excluding_materialization_seconds": cached_reuse_total,
        "three_reuses_with_cache_including_materialization_seconds": cached_end_to_end_total,
        "end_to_end_speedup_including_materialization": uncached_total / cached_end_to_end_total,
        "estimated_break_even_reuses_from_medians": math.ceil(materialize_seconds / median_saving) if median_saving > 0 else None,
        "identical_results": True,
    }


def _partition_benchmark(spark: Any, *, parquet_path: Path, partition_counts: list[int], runs: int) -> dict[str, Any]:
    source = spark.read.parquet(str(parquet_path))
    results = []
    signatures: set[str] = set()
    for requested in partition_counts:
        partitioned = source.repartition(int(requested)).cache()
        materialize_started = time.perf_counter()
        materialized_rows = partitioned.count()
        materialize_seconds = time.perf_counter() - materialize_started
        actual = partitioned.rdd.getNumPartitions()
        durations = []
        for _ in range(runs):
            rows, duration = _timed_collect(representative_query(partitioned))
            durations.append(duration)
            signatures.add(result_signature(rows))
        partitioned.unpersist(blocking=True)
        results.append(
            {
                "requested_partitions": int(requested),
                "actual_partitions": actual,
                "materialized_rows": materialized_rows,
                "repartition_and_materialize_seconds": materialize_seconds,
                "aggregate_timing": timing_summary(durations),
            }
        )
    if len(signatures) != 1:
        raise ValueError("Partition benchmark results differ across partition counts.")
    best = min(results, key=lambda item: item["aggregate_timing"]["median_seconds"])
    best_materialization = min(results, key=lambda item: item["repartition_and_materialize_seconds"])
    return {
        "method": "repartition then cache/materialize; timed aggregate runs scan the prepared in-memory partitions",
        "measured_runs_per_partition_count": runs,
        "results": results,
        "best_median_partition_count": best["actual_partitions"],
        "best_materialization_partition_count": best_materialization["actual_partitions"],
        "identical_results": True,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    formats = report["benchmarks"]["csv_vs_parquet"]
    cache = report["benchmarks"]["cache_reuse"]
    partitions = report["benchmarks"]["partition_count"]
    execution = report["execution_evidence"]
    lines = [
        "# Spark execution và benchmark — Phase 8",
        "",
        f"Sinh lúc: `{report['generated_at']}`.",
        "",
        "## CSV so với Parquet",
        "",
        "Hai định dạng chứa cùng 175.341 dòng Bronze, dùng cùng schema và truy vấn exact-integer. Sau warmup bằng nhau, thứ tự đo được đảo xen kẽ; mỗi định dạng chạy ba lần.",
        "",
        "| Format | Dung lượng (MB) | Partitions đầu vào | Median (s) | Min (s) | Max (s) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name in ("csv", "parquet"):
        item = formats[name]
        timing = item["timing"]
        lines.append(
            f"| {name.upper()} | {item['size_bytes'] / (1024 * 1024):.3f} | {item['input_partitions']} | "
            f"{timing['median_seconds']:.4f} | {timing['min_seconds']:.4f} | {timing['max_seconds']:.4f} |"
        )
    lines.extend(
        [
            "",
            f"Parquet nhanh hơn CSV theo median **{formats['parquet_speedup_over_csv_median']:.2f}x** trong phép đo này; hash kết quả hai định dạng giống nhau.",
            "",
            "## Cache khi tái sử dụng DataFrame",
            "",
            f"Materialize cache {cache['materialized_rows']} dòng mất `{cache['cache_materialization_seconds']:.4f}s` và không được tính vào reuse timing.",
            "",
            "| Chế độ | Median (s) | Min (s) | Max (s) |",
            "|---|---:|---:|---:|",
            f"| Không cache | {cache['without_cache']['median_seconds']:.4f} | {cache['without_cache']['min_seconds']:.4f} | {cache['without_cache']['max_seconds']:.4f} |",
            f"| Có cache | {cache['with_cache']['median_seconds']:.4f} | {cache['with_cache']['min_seconds']:.4f} | {cache['with_cache']['max_seconds']:.4f} |",
            "",
            f"Cache tăng tốc median mỗi action tái sử dụng **{cache['cache_speedup_median']:.2f}x**, nhưng ba lần dùng chỉ tốn `{cache['three_reuses_without_cache_total_seconds']:.4f}s` khi không cache so với `{cache['three_reuses_with_cache_including_materialization_seconds']:.4f}s` nếu tính cả materialize. Với median quan sát được, ước tính cần khoảng **{cache['estimated_break_even_reuses_from_medians']}** lần tái sử dụng để hòa vốn; cache không mặc nhiên nhanh hơn.",
            "",
            "## Số partition",
            "",
            "Mỗi mức được repartition, cache và materialize trước; thời gian dưới đây chỉ đo aggregate trên các partition đã chuẩn bị.",
            "",
            "| Partitions | Materialize (s) | Aggregate median (s) | Min (s) | Max (s) |",
            "|---:|---:|---:|---:|---:|",
        ]
    )
    for item in partitions["results"]:
        timing = item["aggregate_timing"]
        lines.append(
            f"| {item['actual_partitions']} | {item['repartition_and_materialize_seconds']:.4f} | "
            f"{timing['median_seconds']:.4f} | {timing['min_seconds']:.4f} | {timing['max_seconds']:.4f} |"
        )
    lines.extend(
        [
            "",
            f"Aggregate median tốt nhất trong lần chạy này là **{partitions['best_median_partition_count']} partition(s)**, còn repartition/materialize nhanh nhất ở **{partitions['best_materialization_partition_count']} partitions**. Kết quả chỉ áp dụng cho local[2], dữ liệu và truy vấn aggregate nhỏ hiện tại; nhiều partition hơn core có thể tăng overhead scheduler.",
            "",
            "## Bằng chứng execution",
            "",
            f"Spark status tracker ghi **{execution['status_tracker']['job_count']} Job** và **{execution['status_tracker']['unique_stage_count']} stage ID** dưới Adaptive Query Execution. Có {execution['status_tracker']['total_tasks_across_unique_stages']} task được khai báo trên các stage, nhưng chỉ **{execution['status_tracker']['completed_tasks_across_unique_stages']} task** thuộc **{execution['status_tracker']['stages_with_completed_tasks']} stage** thực sự hoàn tất; các stage còn lại là phương án adaptive bị skip, và task thất bại bằng **{execution['status_tracker']['failed_tasks_across_unique_stages']}**. Physical-plan text chứa **{execution['shuffle_exchange_nodes']}** lần xuất hiện `Exchange` trên cả initial/final adaptive plan, thể hiện shuffle do `groupBy`/`orderBy`.",
            "",
            "```text",
            "Action collect()",
            "  -> Job",
            "     -> Stage trước Exchange: đọc từng input partition -> một Task/partition",
            "     -> Exchange (shuffle theo label, proto)",
            "     -> Stage aggregate: một Task/shuffle partition",
            "     -> Exchange/orderBy -> Stage kết quả",
            "",
            "local[2] -> một JVM local executor -> tối đa 2 Task chạy đồng thời trên 2 task slots",
            "```",
            "",
            "Transformation như `filter`, `groupBy`, `orderBy` là lazy; action `collect()` mới tạo Job. Spark tách Job thành Stage tại wide dependency/Exchange, và mỗi partition của Stage tạo một Task. File plan đầy đủ được lưu riêng để đối chiếu.",
            "",
            "## Vì sao dùng Spark",
            "",
            "Tập development 175 nghìn dòng vẫn có thể xử lý bằng Pandas, nên benchmark local này không được dùng để tuyên bố Spark luôn nhanh hơn. Giá trị của Spark ở đồ án là một pipeline thống nhất có schema, lazy execution, partition, cache, shuffle, Parquet pruning và MLlib, có thể mở rộng sang bộ UNSW-NB15 đầy đủ khoảng 2,54 triệu dòng hoặc cluster mà không viết lại thuật toán chính.",
            "",
            "## Giới hạn benchmark",
            "",
            "- Chạy local trên một máy; không đo network shuffle hoặc nhiều executor.",
            "- Ba lần đo sau warmup giảm nhiễu nhưng không loại bỏ hoàn toàn OS filesystem cache và JVM JIT.",
            "- Kết quả partition phụ thuộc truy vấn, kích thước dữ liệu, số core và cấu hình Spark; không phải quy tắc chung.",
            "- Chỉ collect bảng aggregate nhỏ; không đưa toàn bộ dataset về driver.",
            "",
        ]
    )
    return "\n".join(lines)


def run_benchmarks(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    benchmark = config["benchmark"]
    warmups = int(benchmark["warmup_runs"])
    runs = int(benchmark["measured_runs"])
    partition_counts = [int(value) for value in benchmark["partition_counts"]]
    if warmups < 1 or runs < 2 or not partition_counts or any(value < 1 for value in partition_counts):
        raise ValueError("Benchmark requires >=1 warmup, >=2 measured runs and positive partition counts.")

    raw_csv = PROJECT_ROOT / config["paths"]["raw"] / config["dataset"]["files"]["train"]["filename"]
    bronze_train = PROJECT_ROOT / config["paths"]["bronze"] / "train"
    spark = create_spark_session(config, app_name="unsw-nb15-phase8-benchmark")
    try:
        evidence_query = representative_query(spark.read.parquet(str(bronze_train)))
        query_execution = evidence_query._jdf.queryExecution()
        logical_plan = query_execution.logical().toString()
        optimized_plan = query_execution.optimizedPlan().toString()
        physical_plan_before = query_execution.executedPlan().toString()
        evidence_rows, evidence_seconds, tracker_evidence = _tracked_collect(spark, evidence_query)
        physical_plan_after = query_execution.executedPlan().toString()
        evidence_signature = result_signature(evidence_rows)

        format_results = _format_benchmark(
            spark,
            csv_path=raw_csv,
            parquet_path=bronze_train,
            warmups=warmups,
            runs=runs,
        )
        cache_results = _cache_benchmark(spark, parquet_path=bronze_train, runs=runs)
        partition_results = _partition_benchmark(
            spark,
            parquet_path=bronze_train,
            partition_counts=partition_counts,
            runs=runs,
        )
        disk = shutil.disk_usage(PROJECT_ROOT)
        total_memory = _physical_memory_bytes()
        report = {
            "phase": 8,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "benchmark_protocol": {
                "clock": "time.perf_counter",
                "warmup_runs": warmups,
                "measured_runs": runs,
                "aggregate_collection_only": True,
                "full_dataset_collected": False,
            },
            "environment": {
                "platform": platform.platform(),
                "machine": platform.machine(),
                "processor": platform.processor(),
                "logical_cpu_count": os.cpu_count(),
                "physical_memory_bytes": total_memory,
                "python_version": sys.version.split()[0],
                "java_version": str(spark._jvm.java.lang.System.getProperty("java.version")),
                "spark_version": spark.version,
                "spark_master": spark.sparkContext.master,
                "spark_application_id": spark.sparkContext.applicationId,
                "local_task_slots": 2,
                "local_executor_processes": 1,
                "disk_total_bytes": disk.total,
                "disk_free_bytes": disk.free,
                "spark_settings": {
                    key: spark.conf.get(key)
                    for key in ("spark.sql.shuffle.partitions", "spark.default.parallelism", "spark.driver.memory")
                },
            },
            "execution_evidence": {
                "query": "filter valid binary labels -> groupBy(label, proto) -> count/sum bytes -> orderBy",
                "input_partitions": spark.read.parquet(str(bronze_train)).rdd.getNumPartitions(),
                "output_aggregate_rows": len(evidence_rows),
                "source_rows_verified_by_aggregate": sum(int(row["flow_count"]) for row in evidence_rows),
                "result_sha256": evidence_signature,
                "action": "collect small aggregate",
                "action_seconds": evidence_seconds,
                "shuffle_exchange_nodes": physical_plan_after.count("Exchange"),
                "status_tracker": tracker_evidence,
            },
            "benchmarks": {
                "csv_vs_parquet": format_results,
                "cache_reuse": cache_results,
                "partition_count": partition_results,
            },
        }
    finally:
        spark.stop()

    outputs_root = PROJECT_ROOT / config["paths"]["outputs"] / "benchmarks"
    outputs_root.mkdir(parents=True, exist_ok=True)
    metrics_path = outputs_root / "phase8_benchmark.json"
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    plan_path = outputs_root / "representative_query_plan.txt"
    plan_path.write_text(
        "\n".join(
            [
                "LOGICAL PLAN",
                "============",
                logical_plan,
                "",
                "OPTIMIZED LOGICAL PLAN",
                "======================",
                optimized_plan,
                "",
                "PHYSICAL PLAN BEFORE ACTION",
                "===========================",
                physical_plan_before,
                "",
                "PHYSICAL PLAN AFTER ACTION / ADAPTIVE EXECUTION",
                "===============================================",
                physical_plan_after,
                "",
            ]
        ),
        encoding="utf-8",
    )
    document_path = PROJECT_ROOT / "docs" / "SPARK_EXECUTION_AND_BENCHMARK.md"
    document_path.write_text(_render_markdown(report), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run controlled Phase-8 Spark benchmarks.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_benchmarks(arguments.config)

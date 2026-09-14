"""Small Spark experiments used by the guided presentation dashboard."""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.benchmark import _format_benchmark, representative_query, result_signature, track_query_execution


def summarize_data(dataframe: DataFrame) -> dict[str, Any]:
    """Collect only bounded metadata and label aggregates for the first demo step."""
    label_distribution = [
        {"label": int(row["label"]), "count": int(row["count"])}
        for row in dataframe.groupBy("label").count().orderBy("label").collect()
    ]
    return {
        "row_count": int(dataframe.count()),
        "column_count": len(dataframe.columns),
        "columns": list(dataframe.columns),
        "schema": [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": bool(field.nullable),
            }
            for field in dataframe.schema.fields
        ],
        "label_distribution": label_distribution,
    }


def run_format_comparison(
    spark: SparkSession,
    csv_path: str | Path,
    parquet_path: str | Path,
    *,
    warmups: int = 1,
    runs: int = 2,
) -> dict[str, Any]:
    """Run the existing controlled benchmark and shape it for one guided step."""
    benchmark = _format_benchmark(
        spark,
        csv_path=Path(csv_path),
        parquet_path=Path(parquet_path),
        warmups=int(warmups),
        runs=int(runs),
    )
    csv_median = float(benchmark["csv"]["timing"]["median_seconds"])
    parquet_median = float(benchmark["parquet"]["timing"]["median_seconds"])
    return {
        "flow": {
            "input": "Cùng dữ liệu UNSW-NB15 ở CSV và Parquet",
            "spark": "Đọc → filter label → groupBy proto → aggregate → collect",
            "result": "Cùng aggregate, khác chi phí đọc",
        },
        "metrics": [
            {"label": "CSV median", "value": f"{csv_median:.3f}s"},
            {"label": "Parquet median", "value": f"{parquet_median:.3f}s"},
            {"label": "Parquet speedup", "value": f"{benchmark['parquet_speedup_over_csv_median']:.2f}x"},
        ],
        "explanation": (
            "Spark chạy cùng một truy vấn trên hai format. Parquet thường đọc nhanh hơn "
            "vì schema/cột đã được lưu theo dạng máy đọc được; số đo này chỉ đúng cho lần chạy hiện tại."
        ),
        "evidence": {
            "identical_results": bool(benchmark["identical_results"]),
            "csv_result_sha256": benchmark["csv"]["result_sha256"],
            "parquet_result_sha256": benchmark["parquet"]["result_sha256"],
            "csv_size_bytes": int(benchmark["csv"]["size_bytes"]),
            "parquet_size_bytes": int(benchmark["parquet"]["size_bytes"]),
        },
        "identical_results": bool(benchmark["identical_results"]),
        "benchmark": benchmark,
    }


def build_representative_query(dataframe: DataFrame) -> DataFrame:
    """Expose the same query in a named teaching-oriented API."""
    return representative_query(dataframe)


def count_exchange_nodes(dataframe: DataFrame) -> int:
    """Count Exchange nodes in Spark's currently reported physical plan."""
    return dataframe._jdf.queryExecution().executedPlan().toString().count("Exchange")


def run_lazy_evaluation_demo(spark: SparkSession, dataframe: DataFrame) -> dict[str, Any]:
    """Show that transformations build a plan and an action creates a Job."""
    group_id = f"guided-lazy-{uuid.uuid4().hex[:10]}"
    context = spark.sparkContext
    tracker = context.statusTracker()
    query = build_representative_query(dataframe)
    execution = query._jdf.queryExecution()
    context.setJobGroup(group_id, "Guided demo lazy evaluation")
    try:
        before = sorted(int(value) for value in tracker.getJobIdsForGroup(group_id))
        logical_plan = execution.logical().toString()
        optimized_plan = execution.optimizedPlan().toString()
        physical_plan = execution.executedPlan().toString()
        started = time.perf_counter()
        rows = query.collect()
        action_seconds = time.perf_counter() - started
        after = sorted(int(value) for value in tracker.getJobIdsForGroup(group_id))
        return {
            "flow": {
                "input": "DataFrame đã đọc",
                "spark": "filter + groupBy + aggregate + orderBy (chưa chạy)",
                "result": "collect() mới kích hoạt Job",
            },
            "metrics": [
                {"label": "Job trước action", "value": str(len(before))},
                {"label": "Job sau action", "value": str(len(after))},
                {"label": "Action", "value": f"{action_seconds:.3f}s"},
            ],
            "explanation": (
                "Các transformation chỉ tạo kế hoạch. Khi gọi action như collect(), "
                "Spark mới tối ưu plan và gửi Job xuống executor."
            ),
            "evidence": {
                "job_group_id": group_id,
                "job_ids_before_action": before,
                "job_ids_after_action": after,
                "logical_plan": logical_plan,
                "optimized_plan": optimized_plan,
                "physical_plan": physical_plan,
                "result_sha256": result_signature(rows),
            },
            "job_ids_before_action": before,
            "job_ids_after_action": after,
            "result_rows": len(rows),
            "action_seconds": action_seconds,
            "logical_plan": logical_plan,
            "optimized_plan": optimized_plan,
            "physical_plan": physical_plan,
        }
    finally:
        context.setLocalProperty("spark.jobGroup.id", None)


def run_execution_demo(spark: SparkSession, dataframe: DataFrame) -> dict[str, Any]:
    """Collect the Job/Stage/Task/Exchange evidence for the representative query."""
    query = build_representative_query(dataframe)
    execution = query._jdf.queryExecution()
    logical_plan = execution.logical().toString()
    optimized_plan = execution.optimizedPlan().toString()
    rows, action_seconds, tracker = track_query_execution(
        spark,
        query,
        description="Guided demo Job Stage Task Exchange evidence",
    )
    physical_plan = execution.executedPlan().toString()
    exchange_nodes = physical_plan.count("Exchange")
    return {
        "flow": {
            "input": "Các input partition của DataFrame",
            "spark": "Job → Stage → Task; Exchange tạo shuffle",
            "result": "Bằng chứng thực thi từ status tracker",
        },
        "metrics": [
            {"label": "Job", "value": str(tracker["job_count"])},
            {"label": "Stage", "value": str(tracker["unique_stage_count"])},
            {"label": "Task hoàn tất", "value": str(tracker["completed_tasks_across_unique_stages"])},
        ],
        "explanation": (
            "groupBy/orderBy là wide transformation nên dữ liệu phải qua Exchange/shuffle. "
            "Spark chia phần việc thành Stage và mỗi partition tạo Task."
        ),
        "evidence": {
            "logical_plan": logical_plan,
            "optimized_plan": optimized_plan,
            "physical_plan": physical_plan,
            "shuffle_exchange_nodes": exchange_nodes,
            "status_tracker": tracker,
            "result_sha256": result_signature(rows),
        },
        "input_partitions": int(dataframe.rdd.getNumPartitions()),
        "output_aggregate_rows": len(rows),
        "action_seconds": action_seconds,
        "shuffle_exchange_nodes": exchange_nodes,
        "status_tracker": tracker,
        "logical_plan": logical_plan,
        "optimized_plan": optimized_plan,
        "physical_plan": physical_plan,
    }

"""Small Spark experiments used by the guided presentation dashboard."""

from __future__ import annotations

import math
import statistics
import time
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.classification import RandomForestClassifier

from src.benchmark import _format_benchmark, representative_query, result_signature, timing_summary, track_query_execution
from src.evaluate import ranking_metrics, threshold_metrics
from src.features import build_binary_pipeline


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


def run_cache_demo(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    runs: int = 2,
) -> dict[str, Any]:
    """Measure cache materialization separately from repeated cached actions."""
    if runs < 1:
        raise ValueError("Cache demo requires at least one measured reuse run.")

    uncached_durations: list[float] = []
    uncached_signature: str | None = None
    representative_query(dataframe).collect()
    for _ in range(runs):
        started = time.perf_counter()
        rows = representative_query(dataframe).collect()
        uncached_durations.append(time.perf_counter() - started)
        uncached_signature = result_signature(rows)

    cached = dataframe.cache()
    materialize_started = time.perf_counter()
    materialized_rows = int(cached.count())
    materialize_seconds = time.perf_counter() - materialize_started
    cache_storage_level = str(cached.storageLevel)
    cache_was_active = bool(cached.is_cached)
    cached_durations: list[float] = []
    cached_signature: str | None = None
    try:
        for _ in range(runs):
            started = time.perf_counter()
            rows = representative_query(cached).collect()
            cached_durations.append(time.perf_counter() - started)
            cached_signature = result_signature(rows)
    finally:
        cached.unpersist(blocking=True)
    cache_is_active_after_cleanup = bool(cached.is_cached)

    uncached_summary = timing_summary(uncached_durations)
    cached_summary = timing_summary(cached_durations)
    uncached_total = float(sum(uncached_durations))
    cached_reuse_total = float(sum(cached_durations))
    median_saving = float(uncached_summary["median_seconds"] - cached_summary["median_seconds"])
    break_even = math.ceil(materialize_seconds / median_saving) if median_saving > 0 else None
    return {
        "flow": {
            "input": "Một DataFrame Parquet",
            "spark": "Action không cache → persist/cache → materialize → reuse",
            "result": "Tách chi phí nạp cache và chi phí tái sử dụng",
        },
        "metrics": [
            {"label": "Materialize cache", "value": f"{materialize_seconds:.3f}s"},
            {"label": "Median reuse", "value": f"{cached_summary['median_seconds']:.3f}s"},
            {"label": "Hòa vốn ước tính", "value": str(break_even) if break_even else "Chưa đạt"},
        ],
        "explanation": (
            "Cache phải trả chi phí materialize lần đầu. Nó chỉ có lợi khi cùng DataFrame "
            "được dùng lại đủ nhiều lần; kết quả phụ thuộc dữ liệu và máy chạy demo."
        ),
        "evidence": {
            "materialized_rows": materialized_rows,
            "cache_storage_level": cache_storage_level,
            "cache_was_active": cache_was_active,
            "cache_is_active_after_cleanup": cache_is_active_after_cleanup,
            "identical_results": uncached_signature == cached_signature,
        },
        "without_cache": {
            **uncached_summary,
            "total_seconds": uncached_total,
            "result_sha256": uncached_signature,
        },
        "with_cache": {
            **cached_summary,
            "total_seconds": cached_reuse_total,
            "result_sha256": cached_signature,
        },
        "cache_materialization_seconds": materialize_seconds,
        "estimated_break_even_reuses": break_even,
        "cache_was_active": cache_was_active,
        "cache_is_active_after_cleanup": cache_is_active_after_cleanup,
        "identical_results": uncached_signature == cached_signature,
    }


def deterministic_demo_sample(dataframe: DataFrame, *, limit: int, seed: int) -> DataFrame:
    """Select a repeatable, label-aware sample without collecting the dataset."""
    if limit < 2:
        raise ValueError("Demo sample limit must be at least 2.")
    labels = [int(row["label"]) for row in dataframe.select("label").distinct().orderBy("label").collect()]
    if not labels:
        raise ValueError("Demo sample source is empty.")
    per_label = max(1, int(limit) // len(labels))
    ranked = dataframe.withColumn(
        "_demo_rank",
        F.row_number().over(
            Window.partitionBy("label").orderBy(F.xxhash64(F.col("id"), F.lit(int(seed))), F.col("id"))
        ),
    )
    return (
        ranked.filter(F.col("_demo_rank") <= per_label)
        .drop("_demo_rank")
        .orderBy("label", "id")
    )


def build_demo_random_forest(config: dict[str, Any]) -> RandomForestClassifier:
    """Build the small RF used for teaching; it never overwrites the final model."""
    demo = config["demo"]
    return RandomForestClassifier(
        labelCol="label",
        featuresCol=config["modeling"]["features_col"],
        seed=int(config["project"]["seed"]),
        numTrees=int(demo["random_forest_num_trees"]),
        maxDepth=int(demo["random_forest_max_depth"]),
        maxBins=32,
        minInstancesPerNode=1,
        featureSubsetStrategy="sqrt",
        subsamplingRate=1.0,
    )


def run_mllib_demo(
    spark: SparkSession,
    training: DataFrame,
    validation: DataFrame,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Fit a small RF on sampled modeling data and report validation metrics only."""
    demo = config["demo"]
    training_sample = deterministic_demo_sample(
        training,
        limit=int(demo["sample_train_rows"]),
        seed=int(config["project"]["seed"]),
    ).cache()
    validation_sample = deterministic_demo_sample(
        validation,
        limit=int(demo["sample_validation_rows"]),
        seed=int(config["project"]["seed"]) + 1,
    ).cache()
    train_rows = int(training_sample.count())
    validation_rows = int(validation_sample.count())
    if train_rows == 0 or validation_rows == 0:
        raise ValueError("MLlib demo requires non-empty training and validation samples.")
    try:
        pipeline = build_binary_pipeline(config, classifier=build_demo_random_forest(config))
        fit_started = time.perf_counter()
        pipeline_model = pipeline.fit(training_sample)
        fit_seconds = time.perf_counter() - fit_started
        predictions = pipeline_model.transform(validation_sample).cache()
        try:
            prediction_rows = int(predictions.count())
            if prediction_rows != validation_rows:
                raise ValueError("MLlib demo prediction row count mismatch.")
            evaluation = threshold_metrics(predictions, thresholds=[0.60])[0]
            ranking = ranking_metrics(predictions)
            feature_row = predictions.select(config["modeling"]["features_col"]).first()
            feature_dimension = int(feature_row[config["modeling"]["features_col"]].size)
        finally:
            predictions.unpersist(blocking=True)
    finally:
        training_sample.unpersist(blocking=True)
        validation_sample.unpersist(blocking=True)

    return {
        "flow": {
            "input": "Train/validation modeling sample",
            "spark": "Feature Pipeline → RandomForestClassifier → transform",
            "result": "Dự đoán Normal/Attack và metrics validation",
        },
        "metrics": [
            {"label": "F1 validation", "value": f"{evaluation['f1']:.3f}"},
            {"label": "Recall validation", "value": f"{evaluation['recall']:.3f}"},
            {"label": "ROC-AUC", "value": f"{ranking['roc_auc']:.3f}"},
        ],
        "explanation": (
            "MLlib gom nhiều bước tiền xử lý và Random Forest vào một Pipeline. "
            "Đây là model nhỏ để minh họa luồng; kết quả đồ án chính thức nằm ở artifact Phase 6/7."
        ),
        "evidence": {
            "purpose": "educational_sample",
            "official_test_used": False,
            "model_saved": False,
            "feature_dimension": feature_dimension,
            "pipeline_stage_names": [stage.__class__.__name__ for stage in pipeline_model.stages],
        },
        "purpose": "educational_sample",
        "official_test_used": False,
        "model_saved": False,
        "train_rows": train_rows,
        "validation_rows": validation_rows,
        "prediction_rows": prediction_rows,
        "fit_seconds": fit_seconds,
        "pipeline_stage_names": [stage.__class__.__name__ for stage in pipeline_model.stages],
        "feature_dimension": feature_dimension,
        "evaluation_metrics": evaluation,
        "ranking_metrics": ranking,
        "confusion_matrix": {
            key: int(evaluation[key])
            for key in ("true_positive", "false_positive", "true_negative", "false_negative")
        },
    }

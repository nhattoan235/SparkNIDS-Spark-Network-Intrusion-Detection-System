"""Load the locked PipelineModel and run metadata-rich batch inference."""

from __future__ import annotations

import argparse
import json
import re
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.evaluate import ranking_metrics, threshold_metrics
from src.finalize_model import directory_sha256
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


OUTPUT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")


def build_prediction_output(
    transformed: DataFrame,
    *,
    threshold: float,
    run_id: str,
    scored_at: str,
    model_name: str,
    model_sha256: str,
) -> tuple[DataFrame, DataFrame]:
    """Apply the locked threshold and return rich/scalar-output DataFrames."""
    scored = (
        transformed.withColumn("model_prediction_at_0_5", F.col("prediction").cast("int"))
        .withColumn("attack_probability", vector_to_array(F.col("probability"))[1])
        .withColumn("prediction", (F.col("attack_probability") >= F.lit(float(threshold))).cast("int"))
        .withColumn("predicted_class", F.when(F.col("prediction") == 1, "Attack").otherwise("Normal"))
    )
    if "label" not in scored.columns:
        scored = scored.withColumn("label", F.lit(None).cast("int"))
    if "attack_cat" not in scored.columns:
        scored = scored.withColumn("attack_cat", F.lit(None).cast("string"))
    scored = (
        scored.withColumn("actual_class", F.when(F.col("label") == 1, "Attack").when(F.col("label") == 0, "Normal"))
        .withColumn("is_correct", F.when(F.col("label").isNull(), F.lit(None).cast("boolean")).otherwise(F.col("prediction") == F.col("label")))
    )

    descriptive = [
        column
        for column in ("id", "proto", "service", "state", "attack_cat", "label", "source_file")
        if column in scored.columns
    ]
    output = scored.select(
        F.lit(run_id).alias("run_id"),
        F.lit(scored_at).alias("scored_at"),
        F.lit(model_name).alias("model_name"),
        F.lit(model_sha256).alias("model_sha256"),
        F.lit(float(threshold)).alias("decision_threshold"),
        *descriptive,
        "attack_probability",
        "model_prediction_at_0_5",
        "prediction",
        "predicted_class",
        "actual_class",
        "is_correct",
    )
    return scored, output


def _render_final_test_markdown(report: dict[str, Any]) -> str:
    metrics = report["evaluation"]["threshold_metrics"]
    ranking = report["evaluation"]["ranking_metrics"]
    validation = report["model"]["validation_metrics_at_locked_threshold"]
    return "\n".join(
        [
            "# Đánh giá official test và batch inference — Phase 7",
            "",
            f"Sinh lúc: `{report['generated_at']}`; run ID `{report['run_id']}`.",
            "",
            "## Giao thức đánh giá",
            "",
            f"Model `{report['model']['name']}` và threshold **{report['model']['decision_threshold']:.2f}** đã được khóa từ Phase 6 trên validation trước khi official test được mở.",
            "Official test chỉ được evaluate tại threshold đã khóa; không khảo sát hoặc chọn lại threshold trên test.",
            "PipelineModel được nạp từ đĩa trong tiến trình batch độc lập và không huấn luyện lại.",
            "",
            "## Kết quả official test",
            "",
            f"- Số dòng: **{report['input']['rows']}**.",
            f"- Phân phối nhãn: Normal **{report['input']['label_counts']['0']}**, Attack **{report['input']['label_counts']['1']}** (`{report['input']['attack_rate']:.4%}` Attack).",
            f"- Precision: **{metrics['precision']:.6f}**.",
            f"- Recall: **{metrics['recall']:.6f}**.",
            f"- F1-score: **{metrics['f1']:.6f}**.",
            f"- False Positive Rate: **{metrics['false_positive_rate']:.6f}**.",
            f"- Accuracy tham khảo: **{metrics['accuracy']:.6f}**.",
            f"- ROC-AUC: **{ranking['roc_auc']:.6f}**.",
            f"- PR-AUC: **{ranking['pr_auc']:.6f}**.",
            "",
            "### Confusion matrix",
            "",
            "| Nhãn thật \\ Dự đoán | Normal (0) | Attack (1) |",
            "|---|---:|---:|",
            f"| Normal (0) | TN = {metrics['true_negative']} | FP = {metrics['false_positive']} |",
            f"| Attack (1) | FN = {metrics['false_negative']} | TP = {metrics['true_positive']} |",
            "",
            "## Validation so với test",
            "",
            f"Validation tại cùng threshold có F1 `{validation['f1']:.6f}`, Recall `{validation['recall']:.6f}`, FPR `{validation['false_positive_rate']:.6f}`. Chênh lệch test được báo cáo như khả năng tổng quát hóa, không dùng để chỉnh lại model.",
            "",
            "## Artifact batch",
            "",
            f"- Parquet: `{report['outputs']['parquet_path']}`.",
            f"- CSV: `{report['outputs']['csv_path']}`.",
            f"- Model SHA-256: `{report['model']['pipeline_sha256']}`.",
            f"- Load model: `{report['timing']['model_load_seconds']:.3f}s`; transform/count: `{report['timing']['transform_seconds']:.3f}s`; ghi output: `{report['timing']['write_seconds']:.3f}s`.",
            "",
        ]
    )


def run_batch_prediction(
    config_path: str | Path | None = None,
    *,
    input_path: str | Path | None = None,
    output_name: str | None = None,
    evaluate_labels: bool = True,
) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    final_config = config["final_model"]
    model_path = PROJECT_ROOT / final_config["model_path"]
    metadata_path = PROJECT_ROOT / final_config["metadata_path"]
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    current_hash = directory_sha256(model_path)
    if current_hash != metadata["pipeline_sha256"]:
        raise ValueError("Final PipelineModel checksum does not match locked metadata.")

    resolved_input = Path(input_path) if input_path is not None else Path(final_config["official_test_path"])
    if not resolved_input.is_absolute():
        resolved_input = PROJECT_ROOT / resolved_input
    resolved_input = resolved_input.resolve()
    official_test = resolved_input == (PROJECT_ROOT / final_config["official_test_path"]).resolve()
    resolved_name = output_name or ("final_test" if official_test else resolved_input.name)
    if not OUTPUT_NAME_PATTERN.fullmatch(resolved_name):
        raise ValueError("output_name must contain only letters, digits, dot, underscore or hyphen.")

    outputs_root = PROJECT_ROOT / config["paths"]["outputs"]
    final_metrics_path = outputs_root / "metrics" / "phase7_final_test.json"
    guard_path = outputs_root / "metrics" / "phase7_official_test_guard.json"
    if official_test and evaluate_labels:
        if final_metrics_path.exists() or guard_path.exists():
            raise FileExistsError("Official test evaluation is already started/completed; refusing to evaluate it again.")
        guard_path.parent.mkdir(parents=True, exist_ok=True)
        guard_path.write_text(
            json.dumps({"status": "started", "started_at": datetime.now().astimezone().isoformat(timespec="seconds")}, indent=2),
            encoding="utf-8",
        )

    run_id = f"phase7-{uuid.uuid4().hex[:12]}"
    scored_at = datetime.now().astimezone().isoformat(timespec="seconds")
    threshold = float(metadata["decision_threshold"])
    spark = create_spark_session(config, app_name=f"unsw-nb15-batch-{resolved_name}")
    try:
        load_started = time.perf_counter()
        model = PipelineModel.load(str(model_path))
        model_load_seconds = time.perf_counter() - load_started
        stage_types = [stage.__class__.__name__ for stage in model.stages]
        if stage_types != metadata["pipeline_stage_types"]:
            raise ValueError("Reloaded PipelineModel stages differ from locked metadata.")

        source = spark.read.parquet(str(resolved_input))
        transform_started = time.perf_counter()
        transformed = model.transform(source)
        scored, output = build_prediction_output(
            transformed,
            threshold=threshold,
            run_id=run_id,
            scored_at=scored_at,
            model_name=metadata["model_name"],
            model_sha256=current_hash,
        )
        scored = scored.cache()
        output = output.cache()
        input_rows = source.count()
        prediction_rows = output.count()
        transform_seconds = time.perf_counter() - transform_started
        if not input_rows or prediction_rows != input_rows:
            raise ValueError("Batch prediction row-count integrity check failed.")

        evaluation: dict[str, Any] | None = None
        evaluation_started = time.perf_counter()
        if evaluate_labels:
            if "label" not in source.columns:
                raise ValueError("Cannot evaluate a batch without a label column.")
            threshold_result = threshold_metrics(scored, [threshold], score_col="attack_probability")[0]
            evaluation = {
                "dataset": "official test" if official_test else str(resolved_input),
                "threshold_source": "Phase-6 validation only",
                "threshold_metrics": threshold_result,
                "ranking_metrics": ranking_metrics(scored),
            }
        evaluation_seconds = time.perf_counter() - evaluation_started

        parquet_path = outputs_root / "predictions" / f"{resolved_name}_parquet"
        csv_path = outputs_root / "predictions" / f"{resolved_name}_csv"
        write_started = time.perf_counter()
        output.write.mode("overwrite").option("compression", "snappy").parquet(str(parquet_path))
        output.coalesce(1).write.mode("overwrite").option("header", True).csv(str(csv_path))
        parquet_rows = spark.read.parquet(str(parquet_path)).count()
        csv_rows = spark.read.option("header", True).csv(str(csv_path)).count()
        write_seconds = time.perf_counter() - write_started
        if parquet_rows != input_rows or csv_rows != input_rows:
            raise ValueError("Persisted batch outputs failed row-count verification.")

        report = {
            "phase": 7,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "run_id": run_id,
            "input": {
                "path": str(resolved_input),
                "format": "parquet",
                "rows": input_rows,
                "official_test": official_test,
                "labels_evaluated": evaluate_labels,
                **(
                    {
                        "label_counts": {
                            "0": evaluation["threshold_metrics"]["true_negative"] + evaluation["threshold_metrics"]["false_positive"],
                            "1": evaluation["threshold_metrics"]["true_positive"] + evaluation["threshold_metrics"]["false_negative"],
                        },
                        "attack_rate": (
                            evaluation["threshold_metrics"]["true_positive"]
                            + evaluation["threshold_metrics"]["false_negative"]
                        )
                        / input_rows,
                    }
                    if evaluation is not None
                    else {}
                ),
            },
            "model": {
                "name": metadata["model_name"],
                "path": str(model_path),
                "pipeline_sha256": current_hash,
                "loaded_without_training": True,
                "pipeline_stage_types": stage_types,
                "decision_threshold": threshold,
                "threshold_source": "Phase-6 validation only",
                "validation_metrics_at_locked_threshold": metadata["validation_metrics_at_locked_threshold"],
            },
            "evaluation": evaluation,
            "outputs": {
                "columns": output.columns,
                "rows": prediction_rows,
                "parquet_path": str(parquet_path),
                "parquet_verified_rows": parquet_rows,
                "csv_path": str(csv_path),
                "csv_verified_rows": csv_rows,
            },
            "timing": {
                "model_load_seconds": model_load_seconds,
                "transform_seconds": transform_seconds,
                "evaluation_seconds": evaluation_seconds,
                "write_seconds": write_seconds,
            },
            "spark": {
                "version": spark.version,
                "master": spark.sparkContext.master,
                "application_id": spark.sparkContext.applicationId,
            },
        }
        output.unpersist()
        scored.unpersist()
    finally:
        spark.stop()

    if official_test and evaluate_labels:
        final_metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        document_path = PROJECT_ROOT / "docs" / "FINAL_TEST_REPORT.md"
        document_path.write_text(_render_final_test_markdown(report), encoding="utf-8")
        metadata["official_test_evaluated"] = True
        metadata["official_test_evaluation"] = {
            "completed_at": report["generated_at"],
            "run_id": run_id,
            "evaluation_count": 1,
            "metrics_path": str(final_metrics_path),
        }
        metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
        guard_path.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "completed_at": report["generated_at"],
                    "run_id": run_id,
                    "metrics_path": str(final_metrics_path),
                    "evaluation_count": 1,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
    else:
        run_metadata_path = outputs_root / "predictions" / f"{resolved_name}_metadata.json"
        run_metadata_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run batch inference with the locked Phase-7 model.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    parser.add_argument("--input", type=Path, default=None, help="Input Parquet path; defaults to official test.")
    parser.add_argument("--output-name", default=None, help="Safe output basename.")
    parser.add_argument("--no-evaluate", action="store_true", help="Skip label-based metrics for unlabeled/new data.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_batch_prediction(
        arguments.config,
        input_path=arguments.input,
        output_name=arguments.output_name,
        evaluate_labels=not arguments.no_evaluate,
    )

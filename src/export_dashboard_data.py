"""Export bounded aggregates from prediction artifacts for the offline dashboard."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


ALERT_COLUMNS = [
    "id",
    "proto",
    "service",
    "state",
    "attack_cat",
    "label",
    "attack_probability",
    "prediction",
    "predicted_class",
    "actual_class",
    "is_correct",
]


def _collect_rows(dataframe: DataFrame) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in dataframe.collect()]


def _top_distribution(predictions: DataFrame, column: str, limit: int = 15) -> list[dict[str, Any]]:
    return _collect_rows(
        predictions.filter(F.col("prediction") == 1)
        .groupBy(column)
        .agg(
            F.count("*").alias("alert_count"),
            F.avg("attack_probability").alias("avg_attack_probability"),
        )
        .orderBy(F.desc("alert_count"), column)
        .limit(limit)
    )


def run_dashboard_export(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    dashboard_config = config["dashboard"]
    alerts_limit = int(dashboard_config["alerts_limit"])
    error_limit = int(dashboard_config["errors_limit_per_type"])
    if alerts_limit < 1 or error_limit < 1:
        raise ValueError("Dashboard export limits must be positive.")

    outputs_root = PROJECT_ROOT / config["paths"]["outputs"]
    predictions_path = outputs_root / "predictions" / "final_test_parquet"
    final_metrics_path = outputs_root / "metrics" / "phase7_final_test.json"
    final_metrics = json.loads(final_metrics_path.read_text(encoding="utf-8"))
    spark = create_spark_session(config, app_name="unsw-nb15-phase9-dashboard-export")
    try:
        predictions = spark.read.parquet(str(predictions_path)).cache()
        source_rows = predictions.count()
        if source_rows != int(final_metrics["outputs"]["rows"]):
            raise ValueError("Dashboard source row count differs from Phase-7 metadata.")

        alerts = _collect_rows(
            predictions.filter(F.col("prediction") == 1)
            .select(*ALERT_COLUMNS)
            .orderBy(F.desc("attack_probability"), "id")
            .limit(alerts_limit)
        )
        false_positives = _collect_rows(
            predictions.filter((F.col("label") == 0) & (F.col("prediction") == 1))
            .select(*ALERT_COLUMNS)
            .orderBy(F.desc("attack_probability"), "id")
            .limit(error_limit)
        )
        false_negatives = _collect_rows(
            predictions.filter((F.col("label") == 1) & (F.col("prediction") == 0))
            .select(*ALERT_COLUMNS)
            .orderBy("attack_probability", "id")
            .limit(error_limit)
        )
        payload = {
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "source": {
                "prediction_path": str(predictions_path),
                "source_rows": source_rows,
                "prediction_run_id": final_metrics["run_id"],
                "model_name": final_metrics["model"]["name"],
                "model_sha256": final_metrics["model"]["pipeline_sha256"],
                "decision_threshold": final_metrics["model"]["decision_threshold"],
            },
            "limits": {
                "alerts": alerts_limit,
                "errors_per_type": error_limit,
            },
            "alerts": alerts,
            "false_positives": false_positives,
            "false_negatives": false_negatives,
            "alert_distribution": {
                "protocol": _top_distribution(predictions, "proto"),
                "service": _top_distribution(predictions, "service"),
                "attack_category": _top_distribution(predictions, "attack_cat"),
            },
            "collection_policy": {
                "full_prediction_dataset_collected": False,
                "only_limited_rows_and_bounded_aggregates_collected": True,
                "to_pandas_used": False,
            },
        }
        predictions.unpersist()
    finally:
        spark.stop()

    destination = outputs_root / "dashboard" / "alerts_sample.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    manifest = {
        "phase": 9,
        "status": "ready",
        "generated_at": payload["generated_at"],
        "artifacts": {
            "eda": "outputs/dashboard/eda_summary.json",
            "model_comparison": "outputs/dashboard/model_comparison.json",
            "final_test_metrics": "outputs/metrics/phase7_final_test.json",
            "benchmark": "outputs/benchmarks/phase8_benchmark.json",
            "alerts": "outputs/dashboard/alerts_sample.json",
        },
        "alert_rows_exported": len(alerts),
        "false_positive_rows_exported": len(false_positives),
        "false_negative_rows_exported": len(false_negatives),
        "source_prediction_rows": source_rows,
        "offline_ready": True,
        "large_dataset_loaded_by_dashboard": False,
    }
    manifest_path = outputs_root / "dashboard" / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export bounded Phase-9 dashboard artifacts.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_dashboard_export(arguments.config)

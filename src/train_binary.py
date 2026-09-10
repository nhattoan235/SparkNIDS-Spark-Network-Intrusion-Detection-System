"""Train and validate the Phase-5 Logistic Regression baseline."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.ml.classification import LogisticRegression

from src.evaluate import ranking_metrics, select_threshold, threshold_metrics
from src.features import build_binary_pipeline
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


def _metric_line(name: str, value: float) -> str:
    return f"- {name}: **{value:.6f}**."


def _render_markdown(report: dict[str, Any]) -> str:
    selected = report["validation"]["selected_threshold_metrics"]
    default = report["validation"]["default_threshold_metrics"]
    ranking = report["validation"]["ranking_metrics"]
    split = report["data"]
    rows = [
        "# Baseline Logistic Regression — Phase 5",
        "",
        f"Sinh lúc: `{report['generated_at']}`.",
        "",
        "## Phạm vi và chống leakage",
        "",
        f"Pipeline chỉ fit trên modeling train **{split['train_rows']}** dòng và dự đoán validation **{split['validation_rows']}** dòng.",
        f"Official test **{split['designated_test_rows']}** dòng không được transform, evaluate hoặc dùng để chọn threshold.",
        "Không dùng class weight hay tuning trong baseline; các thử nghiệm đó thuộc Phase 6.",
        "",
        "## Kết quả validation tại threshold được chọn",
        "",
        f"Threshold **{selected['threshold']:.2f}** được chọn bằng F1 lớn nhất; nếu hòa thì ưu tiên Recall cao hơn, FPR thấp hơn và gần 0,5 hơn.",
        "",
        _metric_line("Precision", selected["precision"]),
        _metric_line("Recall", selected["recall"]),
        _metric_line("F1-score", selected["f1"]),
        _metric_line("False Positive Rate", selected["false_positive_rate"]),
        _metric_line("Accuracy (tham khảo)", selected["accuracy"]),
        _metric_line("ROC-AUC", ranking["roc_auc"]),
        _metric_line("PR-AUC", ranking["pr_auc"]),
        "",
        "### Confusion matrix",
        "",
        "| Nhãn thật \\ Dự đoán | Normal (0) | Attack (1) |",
        "|---|---:|---:|",
        f"| Normal (0) | TN = {selected['true_negative']} | FP = {selected['false_positive']} |",
        f"| Attack (1) | FN = {selected['false_negative']} | TP = {selected['true_positive']} |",
        "",
        "## So sánh threshold mặc định 0,50",
        "",
        f"Tại 0,50: Precision `{default['precision']:.6f}`, Recall `{default['recall']:.6f}`, F1 `{default['f1']:.6f}`, FPR `{default['false_positive_rate']:.6f}`.",
        "",
        "## Đường threshold trên validation",
        "",
        "| Threshold | Precision | Recall | F1 | FPR | TP | FP | TN | FN |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in report["validation"]["threshold_curve"]:
        rows.append(
            f"| {item['threshold']:.2f} | {item['precision']:.4f} | {item['recall']:.4f} | "
            f"{item['f1']:.4f} | {item['false_positive_rate']:.4f} | {item['true_positive']} | "
            f"{item['false_positive']} | {item['true_negative']} | {item['false_negative']} |"
        )
    rows.extend(
        [
            "",
            "## Vì sao Accuracy không đủ",
            "",
            f"Validation có tỷ lệ Attack `{split['validation_attack_rate']:.4%}`; chỉ đoán Attack đã có thể đạt accuracy tương đương tỷ lệ này. Accuracy cũng gộp FN và FP nên che khuất hai rủi ro khác nhau: bỏ sót tấn công và cảnh báo nhầm. Vì vậy baseline được đọc cùng Recall, FPR, PR-AUC, ROC-AUC và confusion matrix.",
            "",
            "## Thời gian và khả năng tái lập",
            "",
            f"- Fit: **{report['timing']['fit_seconds']:.3f} giây**.",
            f"- Predict validation: **{report['timing']['predict_seconds']:.3f} giây**.",
            f"- Seed của split: `{report['reproducibility']['split_seed']}`; Spark `{report['spark']['version']}`, master `{report['spark']['master']}`.",
            f"- Logistic Regression chạy `{report['model']['total_iterations']}` iteration trên giới hạn `{report['model']['parameters']['max_iter']}`; đạt giới hạn: `{report['model']['reached_max_iterations']}`.",
            "",
        ]
    )
    return "\n".join(rows)


def run_logistic_regression_baseline(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    baseline = config["baseline_logistic_regression"]
    modeling = config["modeling"]
    spark = create_spark_session(config, app_name="unsw-nb15-phase5-logistic-baseline")
    silver_root = PROJECT_ROOT / config["paths"]["silver"]

    try:
        training = spark.read.parquet(str(silver_root / "modeling" / "train")).cache()
        validation = spark.read.parquet(str(silver_root / "modeling" / "validation")).cache()
        train_rows = training.count()
        validation_rows = validation.count()
        designated_test_rows = spark.read.parquet(str(silver_root / "test")).count()
        id_overlap = training.select("id").join(validation.select("id"), on="id", how="inner").count()
        if not train_rows or not validation_rows or id_overlap:
            raise ValueError("Modeling split is empty or has train/validation ID leakage.")

        validation_label_counts = {
            int(row["label"]): int(row["count"])
            for row in validation.groupBy("label").count().collect()
        }
        if set(validation_label_counts) != {0, 1}:
            raise ValueError(f"Validation labels must be binary 0/1: {validation_label_counts}")

        classifier = LogisticRegression(
            labelCol="label",
            featuresCol=modeling["features_col"],
            maxIter=int(baseline["max_iter"]),
            regParam=float(baseline["reg_param"]),
            elasticNetParam=float(baseline["elastic_net_param"]),
            tol=float(baseline["tolerance"]),
            fitIntercept=bool(baseline["fit_intercept"]),
            standardization=False,
            threshold=0.5,
        )
        pipeline = build_binary_pipeline(config, classifier=classifier)
        fit_started = time.perf_counter()
        pipeline_model = pipeline.fit(training)
        fit_seconds = time.perf_counter() - fit_started

        predict_started = time.perf_counter()
        predictions = pipeline_model.transform(validation).cache()
        prediction_rows = predictions.count()
        predict_seconds = time.perf_counter() - predict_started
        if prediction_rows != validation_rows:
            raise ValueError("Validation prediction count differs from validation input count.")

        curve = threshold_metrics(predictions, baseline["threshold_candidates"])
        selection_metric = str(baseline["threshold_selection_metric"])
        selected = select_threshold(curve, metric=selection_metric)
        default = next(item for item in curve if abs(item["threshold"] - 0.5) < 1e-12)
        auc_metrics = ranking_metrics(predictions)

        lr_model = pipeline_model.stages[-1]
        objective_history = [float(value) for value in lr_model.summary.objectiveHistory]
        total_iterations = int(lr_model.summary.totalIterations)
        max_iterations = int(baseline["max_iter"])
        effective_spark_settings = {
            key: spark.conf.get(key)
            for key in (
                "spark.sql.shuffle.partitions",
                "spark.default.parallelism",
                "spark.driver.memory",
            )
        }
        report = {
            "phase": 5,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "task": "binary intrusion detection; Normal=0, Attack=1",
            "data": {
                "fit_dataset": "data/silver/modeling/train",
                "validation_dataset": "data/silver/modeling/validation",
                "train_rows": train_rows,
                "validation_rows": validation_rows,
                "validation_label_counts": validation_label_counts,
                "validation_attack_rate": validation_label_counts[1] / validation_rows,
                "train_validation_id_overlap": id_overlap,
                "designated_test_rows": designated_test_rows,
                "designated_test_untouched": True,
            },
            "model": {
                "name": "LogisticRegression",
                "pipeline_stage_types": [stage.__class__.__name__ for stage in pipeline_model.stages],
                "parameters": {
                    "max_iter": max_iterations,
                    "reg_param": float(baseline["reg_param"]),
                    "elastic_net_param": float(baseline["elastic_net_param"]),
                    "tolerance": float(baseline["tolerance"]),
                    "fit_intercept": bool(baseline["fit_intercept"]),
                    "standardization": False,
                    "training_threshold": 0.5,
                    "class_weight": None,
                },
                "feature_dimension": int(lr_model.numFeatures),
                "total_iterations": total_iterations,
                "reached_max_iterations": total_iterations >= max_iterations,
                "objective_history": objective_history,
                "coefficient_l2_norm": float(lr_model.coefficients.norm(2)),
                "intercept": float(lr_model.intercept),
            },
            "validation": {
                "prediction_rows": prediction_rows,
                "ranking_metrics": auc_metrics,
                "threshold_selection": {
                    "dataset": "validation only",
                    "metric": selection_metric,
                    "tie_breakers": ["higher_recall", "lower_false_positive_rate", "closest_to_0.5"],
                    "selected_threshold": selected["threshold"],
                },
                "default_threshold_metrics": default,
                "selected_threshold_metrics": selected,
                "threshold_curve": curve,
            },
            "timing": {
                "fit_seconds": fit_seconds,
                "predict_seconds": predict_seconds,
            },
            "spark": {
                "version": spark.version,
                "master": spark.sparkContext.master,
                "application_id": spark.sparkContext.applicationId,
                "settings": effective_spark_settings,
            },
            "reproducibility": {
                "split_seed": int(config["project"]["seed"]),
                "split_method": "pmod(xxhash64(id, seed), hash_buckets)",
                "lr_seed_note": "Spark LogisticRegression has no stochastic seed parameter; fixed data split and parameters are recorded.",
            },
        }
        predictions.unpersist()
        training.unpersist()
        validation.unpersist()
    finally:
        spark.stop()

    metrics_path = PROJECT_ROOT / config["paths"]["outputs"] / "metrics" / "phase5_logistic_regression.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    document_path = PROJECT_ROOT / "docs" / "BASELINE_LOGISTIC_REGRESSION.md"
    document_path.write_text(_render_markdown(report), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and validate the Phase-5 Logistic Regression baseline.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_logistic_regression_baseline(arguments.config)

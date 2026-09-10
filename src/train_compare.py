"""Phase-6 bounded model comparison and class-imbalance experiment."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.evaluate import ranking_metrics, select_threshold, threshold_metrics
from src.features import build_binary_pipeline
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


def balanced_class_weights(dataframe: DataFrame, *, label_col: str = "label") -> dict[int, float]:
    """Compute N/(K*n_class) from training labels only."""
    counts = {int(row[label_col]): int(row["count"]) for row in dataframe.groupBy(label_col).count().collect()}
    if set(counts) != {0, 1} or any(value <= 0 for value in counts.values()):
        raise ValueError(f"Balanced binary weights require both labels 0 and 1: {counts}")
    total = sum(counts.values())
    return {label: total / (2.0 * count) for label, count in counts.items()}


def add_class_weights(
    dataframe: DataFrame,
    weights: dict[int, float],
    *,
    label_col: str = "label",
    weight_col: str = "class_weight",
) -> DataFrame:
    """Attach fixed training-derived weights without inspecting validation."""
    if set(weights) != {0, 1} or any(value <= 0 for value in weights.values()):
        raise ValueError("Weights must contain positive values for labels 0 and 1.")
    return dataframe.withColumn(
        weight_col,
        F.when(F.col(label_col) == 0, F.lit(float(weights[0])))
        .when(F.col(label_col) == 1, F.lit(float(weights[1])))
        .otherwise(F.lit(None).cast("double")),
    )


def select_best_candidate(candidates: list[dict[str, Any]], *, metric: str = "f1") -> dict[str, Any]:
    """Select on validation only, with deterministic operational tie-breakers."""
    if not candidates:
        raise ValueError("At least one candidate is required.")
    return sorted(
        candidates,
        key=lambda item: (
            -float(item["selected_threshold_metrics"][metric]),
            -float(item["selected_threshold_metrics"]["recall"]),
            float(item["selected_threshold_metrics"]["false_positive_rate"]),
            float(item["timing"]["predict_seconds"]),
            float(item["timing"]["fit_seconds"]),
            str(item["name"]),
        ),
    )[0]


def _directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _lr_classifier(config: dict[str, Any], *, weight_col: str | None = None) -> LogisticRegression:
    baseline = config["baseline_logistic_regression"]
    modeling = config["modeling"]
    arguments: dict[str, Any] = {
        "labelCol": "label",
        "featuresCol": modeling["features_col"],
        "maxIter": int(baseline["max_iter"]),
        "regParam": float(baseline["reg_param"]),
        "elasticNetParam": float(baseline["elastic_net_param"]),
        "tol": float(baseline["tolerance"]),
        "fitIntercept": bool(baseline["fit_intercept"]),
        "standardization": False,
        "threshold": 0.5,
    }
    if weight_col:
        arguments["weightCol"] = weight_col
    return LogisticRegression(**arguments)


def _rf_classifier(config: dict[str, Any], candidate: dict[str, Any]) -> RandomForestClassifier:
    return RandomForestClassifier(
        labelCol="label",
        featuresCol=config["modeling"]["features_col"],
        seed=int(config["project"]["seed"]),
        numTrees=int(candidate["num_trees"]),
        maxDepth=int(candidate["max_depth"]),
        maxBins=int(candidate["max_bins"]),
        minInstancesPerNode=int(candidate["min_instances_per_node"]),
        featureSubsetStrategy=str(candidate["feature_subset_strategy"]),
        subsamplingRate=float(candidate["subsampling_rate"]),
    )


def _model_details(model: Any) -> dict[str, Any]:
    if model.__class__.__name__ == "LogisticRegressionModel":
        return {
            "total_iterations": int(model.summary.totalIterations),
            "coefficient_l2_norm": float(model.coefficients.norm(2)),
            "coefficient_nonzeros": int(model.coefficients.numNonzeros()),
            "intercept": float(model.intercept),
        }
    return {
        "num_trees": int(model.getNumTrees),
        "total_tree_nodes": int(sum(tree.numNodes for tree in model.trees)),
        "feature_importance_nonzeros": int(model.featureImportances.numNonzeros()),
    }


def _evaluate_candidate(
    *,
    name: str,
    family: str,
    pipeline: Any,
    fit_frame: DataFrame,
    validation: DataFrame,
    validation_rows: int,
    thresholds: list[float],
    selection_metric: str,
    model_path: Path,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    fit_started = time.perf_counter()
    pipeline_model = pipeline.fit(fit_frame)
    fit_seconds = time.perf_counter() - fit_started

    predict_started = time.perf_counter()
    predictions = pipeline_model.transform(validation).cache()
    prediction_rows = predictions.count()
    predict_seconds = time.perf_counter() - predict_started
    if prediction_rows != validation_rows:
        raise ValueError(f"{name}: prediction row count mismatch.")

    curve = threshold_metrics(predictions, thresholds)
    selected = select_threshold(curve, metric=selection_metric)
    default = next(item for item in curve if abs(item["threshold"] - 0.5) < 1e-12)
    auc_metrics = ranking_metrics(predictions)
    classifier_model = pipeline_model.stages[-1]

    save_started = time.perf_counter()
    pipeline_model.write().overwrite().save(str(model_path))
    save_seconds = time.perf_counter() - save_started
    saved_bytes = _directory_size(model_path)
    predictions.unpersist()
    return {
        "name": name,
        "family": family,
        "parameters": parameters,
        "selected_threshold_metrics": selected,
        "default_threshold_metrics": default,
        "ranking_metrics": auc_metrics,
        "threshold_curve": curve,
        "timing": {
            "fit_seconds": fit_seconds,
            "predict_seconds": predict_seconds,
            "candidate_save_seconds": save_seconds,
        },
        "model_artifact": {
            "purpose": "Phase-6 candidate size measurement; not the locked final model.",
            "path": str(model_path),
            "size_bytes": saved_bytes,
            "size_megabytes": saved_bytes / (1024 * 1024),
        },
        "model_details": _model_details(classifier_model),
    }


def _render_markdown(report: dict[str, Any]) -> str:
    selected_name = report["selection"]["selected_candidate"]
    lines = [
        "# So sánh mô hình và xử lý mất cân bằng — Phase 6",
        "",
        f"Sinh lúc: `{report['generated_at']}`.",
        "",
        "## Thiết kế thí nghiệm",
        "",
        f"Bốn candidate dùng cùng modeling train **{report['data']['train_rows']}** dòng và validation **{report['data']['validation_rows']}** dòng. Official test **{report['data']['designated_test_rows']}** dòng không được transform/evaluate.",
        "Trọng số cân bằng chỉ được tính từ train theo `N/(2*n_class)`. Hai cấu hình Random Forest là tuning nhỏ có giới hạn, không phải grid search lớn.",
        "",
        "## Bảng so sánh validation",
        "",
        "| Candidate | Threshold | Precision | Recall | F1 | FPR | ROC-AUC | PR-AUC | Fit (s) | Predict (s) | Size (MB) | FP | FN |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for candidate in report["candidates"]:
        metrics = candidate["selected_threshold_metrics"]
        ranking = candidate["ranking_metrics"]
        timing = candidate["timing"]
        marker = " **(chọn)**" if candidate["name"] == selected_name else ""
        lines.append(
            f"| {candidate['name']}{marker} | {metrics['threshold']:.2f} | {metrics['precision']:.4f} | "
            f"{metrics['recall']:.4f} | {metrics['f1']:.4f} | {metrics['false_positive_rate']:.4f} | "
            f"{ranking['roc_auc']:.4f} | {ranking['pr_auc']:.4f} | {timing['fit_seconds']:.3f} | "
            f"{timing['predict_seconds']:.3f} | {candidate['model_artifact']['size_megabytes']:.3f} | "
            f"{metrics['false_positive']} | {metrics['false_negative']} |"
        )
    selected = next(candidate for candidate in report["candidates"] if candidate["name"] == selected_name)
    metrics = selected["selected_threshold_metrics"]
    lines.extend(
        [
            "",
            "## Lựa chọn mô hình",
            "",
            f"Chọn **{selected_name}** tại threshold **{metrics['threshold']:.2f}** bằng F1 validation cao nhất; tie-break lần lượt là Recall cao hơn, FPR thấp hơn, predict/fit nhanh hơn.",
            f"Candidate được chọn có TP={metrics['true_positive']}, FP={metrics['false_positive']}, TN={metrics['true_negative']}, FN={metrics['false_negative']}; Recall `{metrics['recall']:.6f}` và FPR `{metrics['false_positive_rate']:.6f}`.",
            "",
            "Quyết định chỉ dựa trên validation. Các thư mục model hiện tại là candidate để đo kích thước; Phase 7 sẽ huấn luyện/khóa pipeline cuối, đánh giá test đúng một lần và kiểm tra lưu-nạp.",
            "",
            "## Trọng số lớp",
            "",
            f"- Normal (0): `{report['imbalance_strategy']['class_weights']['0']:.6f}`.",
            f"- Attack (1): `{report['imbalance_strategy']['class_weights']['1']:.6f}`.",
            "",
        ]
    )
    return "\n".join(lines)


def run_model_comparison(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    comparison = config["model_comparison"]
    baseline = config["baseline_logistic_regression"]
    thresholds = [float(value) for value in baseline["threshold_candidates"]]
    selection_metric = str(comparison["selection_metric"])
    weight_col = str(comparison["class_weight_column"])
    spark = create_spark_session(config, app_name="unsw-nb15-phase6-model-comparison")
    silver_root = PROJECT_ROOT / config["paths"]["silver"]
    candidate_root = PROJECT_ROOT / config["paths"]["models"] / "phase6_candidates"

    try:
        training = spark.read.parquet(str(silver_root / "modeling" / "train")).cache()
        validation = spark.read.parquet(str(silver_root / "modeling" / "validation")).cache()
        train_rows = training.count()
        validation_rows = validation.count()
        designated_test_rows = spark.read.parquet(str(silver_root / "test")).count()
        id_overlap = training.select("id").join(validation.select("id"), on="id", how="inner").count()
        if not train_rows or not validation_rows or id_overlap:
            raise ValueError("Modeling split is empty or has train/validation ID leakage.")

        label_counts = {int(row["label"]): int(row["count"]) for row in training.groupBy("label").count().collect()}
        weights = balanced_class_weights(training)
        weighted_training = add_class_weights(training, weights, weight_col=weight_col).cache()
        if weighted_training.filter(F.col(weight_col).isNull()).limit(1).count():
            raise ValueError("A training row did not receive a class weight.")

        candidate_inputs: list[dict[str, Any]] = [
            {
                "name": "logistic_regression_unweighted",
                "family": "LogisticRegression",
                "classifier": _lr_classifier(config),
                "fit_frame": training,
                "parameters": {**baseline, "class_weight": None},
            },
            {
                "name": "logistic_regression_balanced",
                "family": "LogisticRegression",
                "classifier": _lr_classifier(config, weight_col=weight_col),
                "fit_frame": weighted_training,
                "parameters": {**baseline, "class_weight": "balanced N/(2*n_class)", "weight_col": weight_col},
            },
        ]
        for rf_config in comparison["random_forest_candidates"]:
            candidate_inputs.append(
                {
                    "name": str(rf_config["name"]),
                    "family": "RandomForestClassifier",
                    "classifier": _rf_classifier(config, rf_config),
                    "fit_frame": training,
                    "parameters": {**rf_config, "seed": int(config["project"]["seed"]), "class_weight": None},
                }
            )

        candidates = []
        for candidate in candidate_inputs:
            candidates.append(
                _evaluate_candidate(
                    name=candidate["name"],
                    family=candidate["family"],
                    pipeline=build_binary_pipeline(config, classifier=candidate["classifier"]),
                    fit_frame=candidate["fit_frame"],
                    validation=validation,
                    validation_rows=validation_rows,
                    thresholds=thresholds,
                    selection_metric=selection_metric,
                    model_path=candidate_root / candidate["name"],
                    parameters=candidate["parameters"],
                )
            )

        selected = select_best_candidate(candidates, metric=selection_metric)
        report = {
            "phase": 6,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "data": {
                "train_rows": train_rows,
                "validation_rows": validation_rows,
                "train_label_counts": label_counts,
                "train_validation_id_overlap": id_overlap,
                "designated_test_rows": designated_test_rows,
                "designated_test_untouched": True,
            },
            "imbalance_strategy": {
                "name": "balanced inverse-frequency weightCol",
                "formula": "N/(2*n_class)",
                "weights_fit_dataset": "modeling/train only",
                "class_weights": {str(label): value for label, value in weights.items()},
            },
            "search_scope": {
                "candidate_count": len(candidates),
                "candidate_names": [candidate["name"] for candidate in candidates],
                "threshold_candidates": thresholds,
                "bounded_search": True,
            },
            "candidates": candidates,
            "selection": {
                "dataset": "validation only",
                "primary_metric": selection_metric,
                "tie_breakers": ["higher_recall", "lower_false_positive_rate", "faster_predict", "faster_fit"],
                "selected_candidate": selected["name"],
                "selected_threshold": selected["selected_threshold_metrics"]["threshold"],
                "official_test_used": False,
            },
            "spark": {
                "version": spark.version,
                "master": spark.sparkContext.master,
                "application_id": spark.sparkContext.applicationId,
                "settings": {
                    key: spark.conf.get(key)
                    for key in ("spark.sql.shuffle.partitions", "spark.default.parallelism", "spark.driver.memory")
                },
            },
        }
        weighted_training.unpersist()
        training.unpersist()
        validation.unpersist()
    finally:
        spark.stop()

    outputs_root = PROJECT_ROOT / config["paths"]["outputs"]
    metrics_path = outputs_root / "metrics" / "phase6_model_comparison.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    dashboard_path = outputs_root / "dashboard" / "model_comparison.json"
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)
    dashboard_summary = {
        "generated_at": report["generated_at"],
        "selected_candidate": report["selection"]["selected_candidate"],
        "selected_threshold": report["selection"]["selected_threshold"],
        "candidates": [
            {
                "name": item["name"],
                "family": item["family"],
                **item["selected_threshold_metrics"],
                **item["ranking_metrics"],
                "fit_seconds": item["timing"]["fit_seconds"],
                "predict_seconds": item["timing"]["predict_seconds"],
                "model_size_bytes": item["model_artifact"]["size_bytes"],
            }
            for item in report["candidates"]
        ],
    }
    dashboard_path.write_text(json.dumps(dashboard_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    document_path = PROJECT_ROOT / "docs" / "MODEL_COMPARISON.md"
    document_path.write_text(_render_markdown(report), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the bounded Phase-6 model comparison.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_model_comparison(arguments.config)

"""Distributed binary-classification evaluation helpers."""

from __future__ import annotations

import math
from typing import Any, Iterable

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _safe_divide(numerator: int | float, denominator: int | float) -> float:
    return float(numerator / denominator) if denominator else 0.0


def binary_metrics_from_counts(*, tp: int, fp: int, tn: int, fn: int) -> dict[str, Any]:
    """Compute binary metrics with Attack/label=1 as the positive class."""
    counts = {"true_positive": int(tp), "false_positive": int(fp), "true_negative": int(tn), "false_negative": int(fn)}
    if any(value < 0 for value in counts.values()):
        raise ValueError("Confusion-matrix counts must be non-negative.")

    precision = _safe_divide(tp, tp + fp)
    recall = _safe_divide(tp, tp + fn)
    specificity = _safe_divide(tn, tn + fp)
    fpr = _safe_divide(fp, fp + tn)
    f1 = _safe_divide(2.0 * precision * recall, precision + recall)
    total = tp + fp + tn + fn
    return {
        **counts,
        "total": int(total),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "accuracy": _safe_divide(tp + tn, total),
        "false_positive_rate": fpr,
        "specificity": specificity,
        "false_negative_rate": _safe_divide(fn, fn + tp),
        "predicted_attack_rate": _safe_divide(tp + fp, total),
    }


def threshold_metrics(
    predictions: DataFrame,
    thresholds: Iterable[float],
    *,
    label_col: str = "label",
    probability_col: str = "probability",
    score_col: str | None = None,
) -> list[dict[str, Any]]:
    """Calculate a threshold curve in Spark and collect only aggregate counts."""
    candidates = sorted({float(value) for value in thresholds})
    if not candidates or any(not math.isfinite(value) or not 0.0 < value < 1.0 for value in candidates):
        raise ValueError("Threshold candidates must be finite, unique values strictly between 0 and 1.")

    attack_score = F.col(score_col).cast("double") if score_col else vector_to_array(F.col(probability_col))[1]
    scores = predictions.select(F.col(label_col).cast("int").alias("_label"), attack_score.alias("_attack_score"))
    threshold_frame = predictions.sparkSession.createDataFrame([(value,) for value in candidates], "threshold double")
    rows = (
        scores.crossJoin(F.broadcast(threshold_frame))
        .withColumn("_prediction", (F.col("_attack_score") >= F.col("threshold")).cast("int"))
        .groupBy("threshold")
        .agg(
            F.sum(F.when((F.col("_label") == 1) & (F.col("_prediction") == 1), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col("_label") == 0) & (F.col("_prediction") == 1), 1).otherwise(0)).alias("fp"),
            F.sum(F.when((F.col("_label") == 0) & (F.col("_prediction") == 0), 1).otherwise(0)).alias("tn"),
            F.sum(F.when((F.col("_label") == 1) & (F.col("_prediction") == 0), 1).otherwise(0)).alias("fn"),
        )
        .orderBy("threshold")
        .collect()
    )

    result: list[dict[str, Any]] = []
    for row in rows:
        metrics = binary_metrics_from_counts(tp=row.tp, fp=row.fp, tn=row.tn, fn=row.fn)
        result.append({"threshold": float(row.threshold), **metrics})
    return result


def select_threshold(
    curve: Iterable[dict[str, Any]],
    *,
    metric: str = "f1",
) -> dict[str, Any]:
    """Select deterministically: metric, Recall, lower FPR, then closeness to 0.5."""
    candidates = list(curve)
    if not candidates:
        raise ValueError("Threshold curve must not be empty.")
    if metric not in candidates[0]:
        raise ValueError(f"Unknown threshold-selection metric: {metric}")
    return max(
        candidates,
        key=lambda item: (
            float(item[metric]),
            float(item["recall"]),
            -float(item["false_positive_rate"]),
            -abs(float(item["threshold"]) - 0.5),
            -float(item["threshold"]),
        ),
    )


def ranking_metrics(
    predictions: DataFrame,
    *,
    label_col: str = "label",
    raw_prediction_col: str = "rawPrediction",
) -> dict[str, float]:
    """Return threshold-independent ROC-AUC and PR-AUC using Spark evaluators."""
    common = {"labelCol": label_col, "rawPredictionCol": raw_prediction_col}
    return {
        "roc_auc": float(BinaryClassificationEvaluator(metricName="areaUnderROC", **common).evaluate(predictions)),
        "pr_auc": float(BinaryClassificationEvaluator(metricName="areaUnderPR", **common).evaluate(predictions)),
    }

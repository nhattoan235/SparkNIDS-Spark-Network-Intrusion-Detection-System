"""Phase-5 tests for distributed binary metrics and threshold selection."""

from __future__ import annotations

import pytest

from src.evaluate import binary_metrics_from_counts, select_threshold, threshold_metrics
from src.spark_session import create_spark_session


def test_binary_metrics_from_known_confusion_matrix() -> None:
    metrics = binary_metrics_from_counts(tp=80, fp=20, tn=70, fn=30)

    assert metrics["total"] == 200
    assert metrics["precision"] == pytest.approx(0.8)
    assert metrics["recall"] == pytest.approx(80 / 110)
    assert metrics["f1"] == pytest.approx(2 * 0.8 * (80 / 110) / (0.8 + 80 / 110))
    assert metrics["false_positive_rate"] == pytest.approx(20 / 90)
    assert metrics["accuracy"] == pytest.approx(0.75)


def test_threshold_metrics_counts_without_collecting_predictions() -> None:
    spark = create_spark_session(app_name="phase5-threshold-metrics-test")
    try:
        predictions = spark.createDataFrame(
            [(1, 0.9), (1, 0.6), (1, 0.4), (0, 0.8), (0, 0.3), (0, 0.1)],
            "label int, attack_score double",
        )
        curve = threshold_metrics(predictions, [0.5, 0.7], score_col="attack_score")
        at_half = curve[0]
        at_point_seven = curve[1]

        assert (at_half["true_positive"], at_half["false_positive"], at_half["true_negative"], at_half["false_negative"]) == (2, 1, 2, 1)
        assert (at_point_seven["true_positive"], at_point_seven["false_positive"], at_point_seven["true_negative"], at_point_seven["false_negative"]) == (1, 1, 2, 2)
    finally:
        spark.stop()


def test_threshold_selection_uses_documented_tie_breakers() -> None:
    curve = [
        {"threshold": 0.4, "f1": 0.8, "recall": 0.9, "false_positive_rate": 0.2},
        {"threshold": 0.5, "f1": 0.8, "recall": 0.85, "false_positive_rate": 0.1},
        {"threshold": 0.6, "f1": 0.7, "recall": 0.95, "false_positive_rate": 0.3},
    ]

    assert select_threshold(curve, metric="f1")["threshold"] == 0.4

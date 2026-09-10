"""Phase-6 tests for train-only class weights and candidate selection."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from src.spark_session import create_spark_session
from src.train_compare import add_class_weights, balanced_class_weights, select_best_candidate


def test_balanced_class_weights_are_derived_from_training_counts() -> None:
    spark = create_spark_session(app_name="phase6-class-weight-test")
    try:
        training = spark.createDataFrame([(0,), (0,), (1,), (1,), (1,), (1,)], "label int")
        weights = balanced_class_weights(training)
        weighted = add_class_weights(training, weights)
        sums = {
            row.label: row.weight_sum
            for row in weighted.groupBy("label").agg(F.sum("class_weight").alias("weight_sum")).collect()
        }

        assert weights[0] == pytest.approx(1.5)
        assert weights[1] == pytest.approx(0.75)
        assert sums[0] == pytest.approx(sums[1])
    finally:
        spark.stop()


def test_model_selection_uses_f1_then_recall_then_fpr() -> None:
    candidates = [
        {
            "name": "lower_recall",
            "selected_threshold_metrics": {"f1": 0.9, "recall": 0.8, "false_positive_rate": 0.1},
            "timing": {"predict_seconds": 1.0, "fit_seconds": 1.0},
        },
        {
            "name": "higher_recall",
            "selected_threshold_metrics": {"f1": 0.9, "recall": 0.85, "false_positive_rate": 0.2},
            "timing": {"predict_seconds": 2.0, "fit_seconds": 2.0},
        },
        {
            "name": "lower_f1",
            "selected_threshold_metrics": {"f1": 0.89, "recall": 0.99, "false_positive_rate": 0.01},
            "timing": {"predict_seconds": 0.5, "fit_seconds": 0.5},
        },
    ]

    assert select_best_candidate(candidates)["name"] == "higher_recall"

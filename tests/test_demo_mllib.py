"""Tests for the educational, leakage-safe MLlib Random Forest demo."""

from __future__ import annotations

import json

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from src.demo_experiments import build_demo_random_forest, deterministic_demo_sample, run_mllib_demo
from src.features import build_binary_pipeline
from src.spark_session import create_spark_session, load_config
from src.unsw_nb15_schema import UNSW_NB15_SCHEMA


def _row(row_id: int, label: int):
    values = {}
    for field in UNSW_NB15_SCHEMA.fields:
        if isinstance(field.dataType, StringType):
            values[field.name] = "tcp"
        elif isinstance(field.dataType, DoubleType):
            values[field.name] = float(row_id + 1)
        elif isinstance(field.dataType, (IntegerType, LongType)):
            values[field.name] = row_id + 1
    values.update(
        {
            "id": row_id,
            "proto": "tcp" if row_id % 2 else "udp",
            "service": "http" if row_id % 2 else "dns",
            "state": "fin" if row_id % 2 else "int",
            "attack_cat": "Exploits" if label else "Normal",
            "label": label,
        }
    )
    return tuple(values[field.name] for field in UNSW_NB15_SCHEMA.fields)


def test_demo_sample_is_deterministic_and_keeps_both_labels() -> None:
    spark = create_spark_session(app_name="guided-demo-mllib-sample-test")
    try:
        frame = spark.createDataFrame([_row(row_id, row_id % 2) for row_id in range(1, 21)], UNSW_NB15_SCHEMA)

        first = deterministic_demo_sample(frame, limit=10, seed=42)
        second = deterministic_demo_sample(frame, limit=10, seed=42)

        assert [row.id for row in first.select("id").collect()] == [row.id for row in second.select("id").collect()]
        assert first.count() <= 10
        assert {row.label for row in first.select("label").distinct().collect()} == {0, 1}
    finally:
        spark.stop()


def test_demo_random_forest_uses_demo_parameters_and_feature_contract() -> None:
    config = load_config()
    config["demo"]["random_forest_num_trees"] = 2
    config["demo"]["random_forest_max_depth"] = 2
    spark = create_spark_session(app_name="guided-demo-mllib-config-test")
    try:
        classifier = build_demo_random_forest(config)
        pipeline = build_binary_pipeline(config, classifier=classifier)
        final_assembler = next(
            stage
            for stage in reversed(pipeline.getStages())
            if isinstance(stage, VectorAssembler) and stage.getOutputCol() == "features"
        )

        assert isinstance(classifier, RandomForestClassifier)
        assert classifier.getLabelCol() == "label"
        assert classifier.getFeaturesCol() == "features"
        assert classifier.getNumTrees() == 2
        assert classifier.getMaxDepth() == 2
        assert "id" not in final_assembler.getInputCols()
        assert "attack_cat" not in final_assembler.getInputCols()
        assert "label" not in final_assembler.getInputCols()
    finally:
        spark.stop()


def test_mllib_demo_fits_and_reports_validation_only_metrics() -> None:
    spark = create_spark_session(app_name="guided-demo-mllib-fit-test")
    try:
        config = load_config()
        config["demo"]["random_forest_num_trees"] = 2
        config["demo"]["random_forest_max_depth"] = 2
        training = spark.createDataFrame([_row(row_id, row_id % 2) for row_id in range(1, 17)], UNSW_NB15_SCHEMA)
        validation = spark.createDataFrame([_row(row_id, row_id % 2) for row_id in range(101, 109)], UNSW_NB15_SCHEMA)

        result = run_mllib_demo(spark, training, validation, config)

        assert result["official_test_used"] is False
        assert result["model_saved"] is False
        assert result["train_rows"] == 16
        assert result["validation_rows"] == 8
        assert result["pipeline_stage_names"][-1] == "RandomForestClassificationModel"
        assert result["evaluation_metrics"]["f1"] >= 0
        assert set(result["confusion_matrix"]) == {"true_positive", "false_positive", "true_negative", "false_negative"}
        json.dumps(result)
    finally:
        spark.stop()

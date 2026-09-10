"""Phase-4 tests for deterministic splits and leakage-safe feature stages."""

from __future__ import annotations

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from src.features import build_binary_pipeline, build_feature_pipeline, split_train_validation
from src.spark_session import create_spark_session, load_config
from src.unsw_nb15_schema import (
    CATEGORICAL_COLUMNS,
    EXPECTED_COLUMNS,
    IDENTIFIER_COLUMNS,
    NUMERIC_COLUMNS,
    TARGET_COLUMNS,
    UNSW_NB15_SCHEMA,
)


def _row(row_id: int, label: int, **overrides):
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
    values.update(overrides)
    return tuple(values[field.name] for field in UNSW_NB15_SCHEMA.fields)


def test_feature_contract_excludes_identifiers_and_targets() -> None:
    all_groups = set(CATEGORICAL_COLUMNS) | set(NUMERIC_COLUMNS) | set(IDENTIFIER_COLUMNS) | set(TARGET_COLUMNS)
    assert all_groups == set(EXPECTED_COLUMNS)
    assert not (set(NUMERIC_COLUMNS) & set(TARGET_COLUMNS))
    assert not (set(CATEGORICAL_COLUMNS) & set(TARGET_COLUMNS))

    spark = create_spark_session(app_name="phase4-contract-test")
    try:
        pipeline = build_binary_pipeline(load_config())
        assert isinstance(pipeline.getStages()[-1], LogisticRegression)
        final_assembler = next(
            stage
            for stage in reversed(pipeline.getStages())
            if isinstance(stage, VectorAssembler) and stage.getOutputCol() == "features"
        )
        assert "id" not in final_assembler.getInputCols()
        assert "attack_cat" not in final_assembler.getInputCols()
        assert "label" not in final_assembler.getInputCols()
    finally:
        spark.stop()


def test_deterministic_split_is_disjoint_and_complete() -> None:
    spark = create_spark_session(app_name="phase4-split-test")
    try:
        dataframe = spark.range(1, 101).withColumnRenamed("id", "id")
        train_a, validation_a = split_train_validation(
            dataframe,
            validation_fraction=0.2,
            seed=42,
            hash_buckets=1000,
        )
        train_b, validation_b = split_train_validation(
            dataframe,
            validation_fraction=0.2,
            seed=42,
            hash_buckets=1000,
        )
        train_a_ids = {row.id for row in train_a.collect()}
        validation_a_ids = {row.id for row in validation_a.collect()}
        assert train_a_ids == {row.id for row in train_b.collect()}
        assert validation_a_ids == {row.id for row in validation_b.collect()}
        assert train_a_ids.isdisjoint(validation_a_ids)
        assert train_a_ids | validation_a_ids == set(range(1, 101))
    finally:
        spark.stop()


def test_feature_pipeline_keeps_unseen_categories() -> None:
    spark = create_spark_session(app_name="phase4-feature-test")
    try:
        training = spark.createDataFrame(
            [_row(row_id, row_id % 2) for row_id in range(1, 9)],
            UNSW_NB15_SCHEMA,
        )
        validation = spark.createDataFrame(
            [_row(101, 1, proto="unseen_proto", service="unseen_service", state="unseen_state")],
            UNSW_NB15_SCHEMA,
        )
        model = build_feature_pipeline(load_config()).fit(training)
        transformed = model.transform(validation)
        feature = transformed.select("features").first()["features"]

        assert isinstance(transformed.schema["features"].dataType, VectorUDT)
        assert feature.size > len(NUMERIC_COLUMNS)
        for indexer_model, unseen_value in zip(
            model.stages[: len(CATEGORICAL_COLUMNS)],
            ("unseen_proto", "unseen_service", "unseen_state"),
            strict=True,
        ):
            assert unseen_value not in indexer_model.labels
    finally:
        spark.stop()

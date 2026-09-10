"""Phase-2 tests for schema enforcement and the committed deterministic sample."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.spark_session import PROJECT_ROOT, create_spark_session
from src.unsw_nb15_schema import EXPECTED_COLUMNS, UNSW_NB15_SCHEMA
from src.validate_data import read_header, read_unsw_csv, validate_header


def test_explicit_schema_covers_designated_split() -> None:
    assert len(UNSW_NB15_SCHEMA.fields) == 45
    assert tuple(field.name for field in UNSW_NB15_SCHEMA.fields) == EXPECTED_COLUMNS
    assert UNSW_NB15_SCHEMA["label"].dataType.simpleString() == "int"
    assert UNSW_NB15_SCHEMA["attack_cat"].dataType.simpleString() == "string"


def test_header_validation_rejects_reordered_columns(tmp_path: Path) -> None:
    invalid_csv = tmp_path / "invalid.csv"
    invalid_csv.write_text("label,id\n0,1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unexpected CSV header"):
        validate_header(invalid_csv)


def test_fixed_sample_is_balanced_and_uses_explicit_types() -> None:
    sample_directory = PROJECT_ROOT / "data" / "samples" / "unsw_nb15_sample_csv"
    sample_parts = list(sample_directory.glob("part-*.csv"))
    assert len(sample_parts) == 1
    assert read_header(sample_parts[0]) == EXPECTED_COLUMNS

    spark = create_spark_session(app_name="phase2-sample-test")
    try:
        dataframe = read_unsw_csv(spark, sample_parts[0])
        counts = {row["label"]: row["count"] for row in dataframe.groupBy("label").count().collect()}
        assert dataframe.count() == 200
        assert counts == {0: 100, 1: 100}
        assert [field.dataType for field in dataframe.schema.fields] == [
            field.dataType for field in UNSW_NB15_SCHEMA.fields
        ]
    finally:
        spark.stop()


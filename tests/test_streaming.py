"""Phase-10 tests for the fixed-schema stream and micro-batch contract."""

from __future__ import annotations

import pytest

from src.streaming_predict import (
    build_streaming_source,
    summarize_micro_batch,
    validate_stream_paths,
)
from src.spark_session import create_spark_session
from src.unsw_nb15_schema import EXPECTED_COLUMNS


def test_stream_source_uses_explicit_schema(tmp_path) -> None:
    spark = create_spark_session(app_name="phase10-fixed-schema-test")
    try:
        source = build_streaming_source(spark, tmp_path / "input", max_files_per_trigger=1)
        assert source.isStreaming
        assert source.columns == [*EXPECTED_COLUMNS, "source_file"]
        assert source.schema["id"].dataType.simpleString() == "int"
        assert source.schema["sbytes"].dataType.simpleString() == "bigint"
        assert spark.conf.get("spark.sql.streaming.checkpointFileManagerClass").endswith(
            "FileSystemBasedCheckpointFileManager"
        )
    finally:
        spark.stop()


def test_micro_batch_summary_counts_alerts() -> None:
    spark = create_spark_session(app_name="phase10-summary-test")
    try:
        output = spark.createDataFrame(
            [
                (1, "batch.parquet", 0.10, 0),
                (2, "batch.parquet", 0.80, 1),
                (3, "batch.parquet", 0.90, 1),
            ],
            "id int, source_file string, attack_probability double, prediction int",
        )
        summary = summarize_micro_batch(
            output,
            stream_batch_id=7,
            query_run_id="test-run",
            processed_at="2026-09-10T00:00:00+07:00",
        )
        assert summary["input_rows"] == 3
        assert summary["alert_rows"] == 2
        assert summary["normal_rows"] == 1
        assert summary["alert_rate"] == pytest.approx(2 / 3)
        assert summary["max_attack_probability"] == pytest.approx(0.9)
        assert summary["source_file_count"] == 1
    finally:
        spark.stop()


def test_stream_paths_must_not_overlap(tmp_path) -> None:
    with pytest.raises(ValueError, match="must not overlap"):
        validate_stream_paths(
            tmp_path / "source",
            tmp_path / "source" / "predictions",
            tmp_path / "summaries",
            tmp_path / "checkpoint",
        )

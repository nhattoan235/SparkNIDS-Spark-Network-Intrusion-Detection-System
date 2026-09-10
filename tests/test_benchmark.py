"""Phase-8 tests for benchmark aggregation and timing summaries."""

from __future__ import annotations

import pytest

from src.benchmark import representative_query, result_signature, timing_summary
from src.spark_session import create_spark_session


def test_timing_summary_uses_median_and_preserves_runs() -> None:
    summary = timing_summary([3.0, 1.0, 2.0])

    assert summary["runs"] == 3
    assert summary["seconds"] == [3.0, 1.0, 2.0]
    assert summary["median_seconds"] == 2.0
    assert summary["mean_seconds"] == 2.0


def test_representative_query_has_exact_aggregate_and_stable_signature() -> None:
    spark = create_spark_session(app_name="phase8-representative-query-test")
    try:
        frame = spark.createDataFrame(
            [(0, "tcp", 10, 20), (0, "tcp", 5, 7), (1, "udp", 3, 4), (1, "tcp", 8, 9)],
            "label int, proto string, sbytes long, dbytes long",
        )
        rows = representative_query(frame).collect()

        assert sum(row.flow_count for row in rows) == 4
        assert sum(row.source_bytes for row in rows) == 26
        assert sum(row.destination_bytes for row in rows) == 40
        assert result_signature(rows) == result_signature(list(rows))
        assert "Exchange" in representative_query(frame)._jdf.queryExecution().executedPlan().toString()
    finally:
        spark.stop()


def test_timing_summary_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match="At least one"):
        timing_summary([])

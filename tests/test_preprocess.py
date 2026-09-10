"""Phase-3 tests for deterministic cleaning and Spark SQL aggregates."""

from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from src.eda import run_sql_queries
from src.preprocess import audit_dataframe, clean_dataframe
from src.spark_session import create_spark_session
from src.unsw_nb15_schema import UNSW_NB15_SCHEMA


def _row(**overrides):
    values = {}
    for field in UNSW_NB15_SCHEMA.fields:
        if isinstance(field.dataType, StringType):
            values[field.name] = "tcp"
        elif isinstance(field.dataType, DoubleType):
            values[field.name] = 1.0
        elif isinstance(field.dataType, (IntegerType, LongType)):
            values[field.name] = 1
    values.update({"id": 1, "proto": " TCP ", "service": "-", "state": " FIN "})
    values.update({"attack_cat": "Normal", "label": 0})
    values.update(overrides)
    return tuple(values[field.name] for field in UNSW_NB15_SCHEMA.fields)


def test_clean_dataframe_handles_invalid_values_and_targets() -> None:
    spark = create_spark_session(app_name="phase3-cleaning-test")
    try:
        dirty_rows = [
            _row(),
            _row(id=2, label=1, attack_cat="Exploits", dur=None, rate=-3.0, sload=float("inf")),
            _row(id=3, label=2, attack_cat="Exploits"),
            _row(),
        ]
        dirty = spark.createDataFrame(dirty_rows, UNSW_NB15_SCHEMA)
        before = audit_dataframe(dirty)
        cleaned = clean_dataframe(dirty)
        after = audit_dataframe(cleaned)
        rows = {row.id: row.asDict() for row in cleaned.collect()}

        assert before["duplicate_row_count"] == 1
        assert before["invalid_target_row_count"] == 1
        assert before["invalid_numeric_counts"]["dur"] == 1
        assert before["invalid_numeric_counts"]["rate"] == 1
        assert before["invalid_numeric_counts"]["sload"] == 1
        assert before["missing_categorical_counts"]["service"] == 4
        assert after["row_count"] == 2
        assert after["duplicate_row_count"] == 0
        assert after["invalid_target_row_count"] == 0
        assert all(value == 0 for value in after["invalid_numeric_counts"].values())
        assert rows[1]["proto"] == "tcp"
        assert rows[1]["service"] == "__unknown__"
        assert rows[1]["state"] == "fin"
        assert rows[2]["dur"] == 0.0
        assert rows[2]["rate"] == 0.0
        assert rows[2]["sload"] == 0.0
    finally:
        spark.stop()


def test_spark_sql_eda_has_bounded_named_results() -> None:
    spark = create_spark_session(app_name="phase3-sql-test")
    try:
        rows = [
            _row(id=1, label=0, attack_cat="Normal", proto="tcp", service="http", state="fin"),
            _row(id=2, label=1, attack_cat="Exploits", proto="tcp", service="http", state="fin"),
            _row(id=3, label=1, attack_cat="Generic", proto="udp", service="dns", state="int"),
        ]
        dataframe = clean_dataframe(spark.createDataFrame(rows, UNSW_NB15_SCHEMA))
        results, queries = run_sql_queries(spark, dataframe, top_n=2)

        assert len(queries) >= 3
        assert sum(row["flow_count"] for row in results["class_distribution"]) == 3
        assert len(results["protocol_attack_rate"]) <= 2
        assert {row["traffic_class"] for row in results["class_distribution"]} == {
            "Attack",
            "Normal",
        }
    finally:
        spark.stop()


"""Phase-1 tests for configuration and the local Spark runtime."""

from __future__ import annotations

from src.spark_session import configure_runtime_environment, create_spark_session, load_config


def test_default_config_has_required_local_settings() -> None:
    config = load_config()

    assert config["project"]["seed"] == 42
    assert config["spark"]["master"].startswith("local[")
    assert int(config["spark"]["settings"]["spark.sql.shuffle.partitions"]) > 0


def test_runtime_uses_current_python_for_workers(monkeypatch) -> None:
    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)

    configure_runtime_environment()

    import os
    import sys

    assert os.environ["PYSPARK_PYTHON"] == sys.executable
    assert os.environ["PYSPARK_DRIVER_PYTHON"] == sys.executable


def test_spark_dataframe_action() -> None:
    config = load_config()
    spark = create_spark_session(config, app_name="phase1-pytest-smoke")
    try:
        dataframe = spark.createDataFrame([(1, "Normal"), (2, "Attack")], ["id", "label_name"])
        assert dataframe.filter("id >= 1").count() == 2
        assert {row.label_name for row in dataframe.select("label_name").collect()} == {
            "Normal",
            "Attack",
        }
    finally:
        spark.stop()

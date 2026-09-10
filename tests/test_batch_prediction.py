"""Phase-7 tests for model integrity hashing and locked-threshold output."""

from __future__ import annotations

from pyspark.ml.linalg import Vectors

from src.finalize_model import directory_sha256
from src.predict_batch import build_prediction_output
from src.spark_session import create_spark_session


def test_directory_sha256_is_stable_and_content_sensitive(tmp_path) -> None:
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    first = model_dir / "first.txt"
    first.write_text("one", encoding="utf-8")
    original = directory_sha256(model_dir)

    assert directory_sha256(model_dir) == original
    first.write_text("two", encoding="utf-8")
    assert directory_sha256(model_dir) != original


def test_locked_threshold_controls_exported_prediction() -> None:
    spark = create_spark_session(app_name="phase7-output-contract-test")
    try:
        transformed = spark.createDataFrame(
            [
                (1, "tcp", "http", "fin", "Normal", 0, Vectors.dense([0.45, 0.55]), 1.0),
                (2, "tcp", "http", "fin", "Exploits", 1, Vectors.dense([0.35, 0.65]), 1.0),
            ],
            ["id", "proto", "service", "state", "attack_cat", "label", "probability", "prediction"],
        )
        _, output = build_prediction_output(
            transformed,
            threshold=0.60,
            run_id="test-run",
            scored_at="2026-09-10T00:00:00+07:00",
            model_name="test-model",
            model_sha256="abc123",
        )
        rows = output.orderBy("id").collect()

        assert rows[0].model_prediction_at_0_5 == 1
        assert rows[0].prediction == 0
        assert rows[0].predicted_class == "Normal"
        assert rows[0].is_correct is True
        assert rows[1].prediction == 1
        assert rows[1].predicted_class == "Attack"
        assert rows[1].is_correct is True
        assert "attack_probability" in output.columns
        assert "model_sha256" in output.columns
    finally:
        spark.stop()

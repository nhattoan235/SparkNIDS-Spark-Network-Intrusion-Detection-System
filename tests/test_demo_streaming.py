"""Integration coverage for the guided Structured Streaming restart demo."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.spark_session import load_config
from src.streaming_predict import run_phase10_demo


def test_streaming_demo_restarts_with_same_checkpoint_and_only_reads_new_file(tmp_path: Path) -> None:
    config = load_config()
    if not (Path(config["final_model"]["model_path"]).exists() and Path(config["final_model"]["metadata_path"]).exists()):
        pytest.skip("Locked final model is required for the streaming integration demo.")

    report = run_phase10_demo(
        config,
        run_name="guided-streaming-test",
        rows_per_batch=2,
        input_root=tmp_path / "input",
        output_root=tmp_path / "output",
    )

    checks = report["checkpoint"]["restart_checks"]
    assert checks["same_persistent_query_id"] is True
    assert checks["different_query_run_id"] is True
    assert checks["only_new_file_processed_after_restart"] is True
    assert checks["no_duplicate_ids"] is True
    assert report["outputs"]["prediction_rows"] == 6

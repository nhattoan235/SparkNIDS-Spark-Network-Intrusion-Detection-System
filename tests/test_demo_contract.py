"""Tests for the guided Spark demo artifact contract."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.demo_contract import (
    DEMO_STEP_IDS,
    new_demo_status,
    set_step_failed,
    set_step_result,
    set_step_running,
    write_json_atomic,
    write_latest_pointer,
)


def test_demo_status_has_seven_steps_in_presentation_order() -> None:
    status = new_demo_status("run-1", "demo", "http://127.0.0.1:4040")

    assert DEMO_STEP_IDS == ("data", "parquet", "lazy", "execution", "cache", "mllib", "streaming")
    assert [step["id"] for step in status["steps"]] == list(DEMO_STEP_IDS)
    assert all(step["status"] == "pending" for step in status["steps"])


def test_step_updates_validate_ids_and_statuses() -> None:
    status = new_demo_status("run-1", "demo", None)

    set_step_running(status, "data")
    set_step_result(status, "data", {"summary": "ok"})
    set_step_failed(status, "parquet", ValueError("bad input"), recovery_hint="rerun ingest")

    assert status["steps"][0]["status"] == "succeeded"
    assert status["steps"][0]["result"] == {"summary": "ok"}
    assert status["steps"][1]["status"] == "failed"
    assert status["steps"][1]["error"]["type"] == "ValueError"
    assert status["steps"][1]["error"]["recovery_hint"] == "rerun ingest"

    with pytest.raises(ValueError, match="Unknown demo step"):
        set_step_running(status, "unknown")


def test_atomic_write_replaces_previous_json(tmp_path: Path) -> None:
    target = tmp_path / "run" / "status.json"
    write_json_atomic(target, {"version": 1, "old": True})
    write_json_atomic(target, {"version": 2, "new": True})

    assert json.loads(target.read_text(encoding="utf-8")) == {"version": 2, "new": True}
    assert list(target.parent.glob("*.tmp")) == []


def test_latest_pointer_uses_relative_status_path(tmp_path: Path) -> None:
    output_root = tmp_path / "outputs" / "demo"
    status_path = output_root / "runs" / "run-1" / "status.json"

    pointer = write_latest_pointer(output_root, status_path)

    assert pointer == output_root / "latest.json"
    assert json.loads(pointer.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "status_path": "runs/run-1/status.json",
    }

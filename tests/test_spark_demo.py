"""Tests for the guided Spark demo runner orchestration."""

from __future__ import annotations

from src.demo_contract import new_demo_status
from src.spark_demo import build_parser, execute_steps


def test_demo_parser_supports_guided_run_options() -> None:
    arguments = build_parser().parse_args(
        ["--mode", "full", "--step", "mllib", "--hold-seconds", "7", "--config", "demo.yaml"]
    )

    assert arguments.mode == "full"
    assert arguments.step == "mllib"
    assert arguments.hold_seconds == 7
    assert str(arguments.config) == "demo.yaml"


def test_execute_steps_records_progress_and_isolates_failure() -> None:
    status = new_demo_status("run-1", "demo", None)
    calls: list[str] = []

    def successful_step():
        calls.append("data")
        return {"flow": {}, "metrics": [], "explanation": "ok", "evidence": {}}

    def failed_step():
        calls.append("parquet")
        raise RuntimeError("input missing")

    execute_steps(
        status,
        ["data", "parquet"],
        {"data": successful_step, "parquet": failed_step},
    )

    assert calls == ["data", "parquet"]
    assert status["steps"][0]["status"] == "succeeded"
    assert status["steps"][1]["status"] == "failed"
    assert status["steps"][1]["error"]["message"] == "input missing"
    assert status["active_step"] is None

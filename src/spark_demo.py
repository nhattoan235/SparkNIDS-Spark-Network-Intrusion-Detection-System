"""Command-line runner for the seven-step guided Apache Spark presentation."""

from __future__ import annotations

import argparse
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from src.demo_contract import (
    DEMO_STEP_IDS,
    new_demo_status,
    set_step_failed,
    set_step_result,
    set_step_running,
    write_json_atomic,
    write_latest_pointer,
)
from src.demo_experiments import (
    run_cache_demo,
    run_execution_demo,
    run_format_comparison,
    run_lazy_evaluation_demo,
    run_mllib_demo,
    summarize_data,
)
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.streaming_predict import run_phase10_demo


STEP_CHOICES = ("all", *DEMO_STEP_IDS)
StepFunction = Callable[[], dict[str, Any]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the seven-step guided Spark IDS demo.")
    parser.add_argument("--mode", choices=("demo", "full"), default="demo")
    parser.add_argument("--step", choices=STEP_CHOICES, default="all")
    parser.add_argument("--hold-seconds", type=int, default=None)
    parser.add_argument("--config", type=Path, default=None)
    return parser


def execute_steps(
    status: dict[str, Any],
    step_ids: Iterable[str],
    step_functions: dict[str, StepFunction],
    *,
    on_update: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run selected steps in order and keep later steps available after one failure."""
    for step_id in step_ids:
        set_step_running(status, step_id)
        if on_update:
            on_update(status)
        try:
            result = step_functions[step_id]()
            set_step_result(status, step_id, result)
        except Exception as error:  # one teaching step must not erase prior evidence
            set_step_failed(status, step_id, error)
        if on_update:
            on_update(status)
    return status


def _resolve_project_path(value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def _require_paths(config: dict[str, Any], step_id: str) -> None:
    paths = config["paths"]
    requirements = {
        "data": [_resolve_project_path(paths["silver"]) / "train"],
        "parquet": [
            _resolve_project_path(paths["raw"]) / config["dataset"]["files"]["train"]["filename"],
            _resolve_project_path(paths["bronze"]) / "train",
        ],
        "lazy": [_resolve_project_path(paths["bronze"]) / "train"],
        "execution": [_resolve_project_path(paths["bronze"]) / "train"],
        "cache": [_resolve_project_path(paths["bronze"]) / "train"],
        "mllib": [
            _resolve_project_path(paths["silver"]) / "modeling" / "train",
            _resolve_project_path(paths["silver"]) / "modeling" / "validation",
        ],
        "streaming": [
            _resolve_project_path(config["final_model"]["model_path"]),
            _resolve_project_path(config["final_model"]["metadata_path"]),
        ],
    }
    missing = [str(path) for path in requirements[step_id] if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Step '{step_id}' is missing required artifact(s): {', '.join(missing)}")


def _data_step(spark, config: dict[str, Any]) -> dict[str, Any]:
    frame = spark.read.parquet(str(_resolve_project_path(config["paths"]["silver"]) / "train"))
    summary = summarize_data(frame)
    return {
        "flow": {
            "input": "Silver Parquet UNSW-NB15",
            "spark": "DataFrame đọc schema + aggregate label",
            "result": f"{summary['row_count']:,} network flow",
        },
        "metrics": [
            {"label": "Network flow", "value": f"{summary['row_count']:,}"},
            {"label": "Cột", "value": str(summary["column_count"])},
            {"label": "Nhãn", "value": str(len(summary["label_distribution"]))},
        ],
        "explanation": "Mỗi dòng là một network flow; label 0 là Normal và label 1 là Attack.",
        "evidence": {
            "schema": summary["schema"],
            "columns": summary["columns"],
            "label_distribution": summary["label_distribution"],
        },
        **summary,
    }


def _streaming_step(config: dict[str, Any], run_id: str) -> dict[str, Any]:
    demo = config["demo"]
    report = run_phase10_demo(
        config,
        run_name=f"guided-{run_id}",
        rows_per_batch=int(demo["stream_rows_per_batch"]),
        input_root=_resolve_project_path(config["paths"]["streaming_input"] if "streaming_input" in config["paths"] else config["streaming"]["input_root"]) / "guided",
        output_root=_resolve_project_path(config["paths"]["outputs"]) / "demo" / "runtime" / run_id / "streaming",
    )
    checks = report["checkpoint"]["restart_checks"]
    return {
        "flow": {
            "input": "Các file Parquet đến theo từng đợt",
            "spark": "Structured Streaming + foreachBatch + checkpoint",
            "result": "Micro-batch mới sau restart",
        },
        "metrics": [
            {"label": "File đã xử lý", "value": str(report["input"]["published_files"])},
            {"label": "Prediction rows", "value": str(report["outputs"]["prediction_rows"])},
            {"label": "File cũ không lặp", "value": "Có" if checks["no_duplicate_ids"] else "Không"},
        ],
        "explanation": "Checkpoint giúp query tiếp tục từ trạng thái cũ và chỉ nhận file mới sau restart.",
        "evidence": {
            "checkpoint_path": report["checkpoint"]["path"],
            "restart_checks": checks,
            "first_run": report["first_run"],
            "restart_run": report["restart_run"],
        },
        "streaming_report": report,
    }


def _hold_spark_ui(seconds: int) -> None:
    if seconds <= 0:
        return
    print(f"Spark UI đang mở trong {seconds} giây. Mở http://127.0.0.1:4040 để xem Jobs/Stages.")
    deadline = time.monotonic() + seconds
    while True:
        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            break
        print(f"Còn khoảng {remaining} giây...")
        time.sleep(min(10, remaining))


def run_demo(
    config_path: str | Path | dict[str, Any] | None = None,
    *,
    mode: str = "demo",
    step: str = "all",
    hold_seconds: int | None = None,
) -> dict[str, Any]:
    """Run the requested guided steps and publish a live/offline status artifact."""
    config = config_path if isinstance(config_path, dict) else load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    if mode not in {"demo", "full"}:
        raise ValueError(f"Unknown demo mode: {mode}")
    if step not in STEP_CHOICES:
        raise ValueError(f"Unknown demo step selection: {step}")
    selected_steps = list(DEMO_STEP_IDS) if step == "all" else [step]
    run_id = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{mode}-{uuid.uuid4().hex[:6]}"
    output_root = _resolve_project_path(config["paths"]["outputs"]) / "demo"
    run_root = output_root / "runs" / run_id
    status_path = run_root / "status.json"
    status = new_demo_status(run_id, mode, None)

    def persist(current: dict[str, Any]) -> None:
        write_json_atomic(status_path, current)
        write_latest_pointer(output_root, status_path)

    persist(status)
    spark = None
    try:
        batch_steps = [item for item in selected_steps if item != "streaming"]
        for step_id in selected_steps:
            _require_paths(config, step_id)

        if batch_steps:
            spark = create_spark_session(config, app_name=f"spark-guided-demo-{run_id}")
            status["spark_ui_url"] = spark.sparkContext.uiWebUrl or "http://127.0.0.1:4040"
            persist(status)
            bronze_train = _resolve_project_path(config["paths"]["bronze"]) / "train"
            functions: dict[str, StepFunction] = {
                "data": lambda: _data_step(spark, config),
                "parquet": lambda: run_format_comparison(
                    spark,
                    _resolve_project_path(config["paths"]["raw"])
                    / config["dataset"]["files"]["train"]["filename"],
                    bronze_train,
                    warmups=1,
                    runs=int(config["demo"]["measured_runs"]),
                ),
                "lazy": lambda: run_lazy_evaluation_demo(spark, spark.read.parquet(str(bronze_train))),
                "execution": lambda: run_execution_demo(spark, spark.read.parquet(str(bronze_train))),
                "cache": lambda: run_cache_demo(spark, spark.read.parquet(str(bronze_train)), runs=int(config["demo"]["measured_runs"])),
                "mllib": lambda: run_mllib_demo(
                    spark,
                    spark.read.parquet(str(_resolve_project_path(config["paths"]["silver"]) / "modeling" / "train")),
                    spark.read.parquet(str(_resolve_project_path(config["paths"]["silver"]) / "modeling" / "validation")),
                    config,
                ),
            }
            execute_steps(status, batch_steps, functions, on_update=persist)
            if "streaming" in selected_steps:
                _hold_spark_ui(int(hold_seconds if hold_seconds is not None else config["demo"]["hold_seconds"]))
            spark.stop()
            spark = None

        if "streaming" in selected_steps:
            execute_steps(
                status,
                ["streaming"],
                {"streaming": lambda: _streaming_step(config, run_id)},
                on_update=persist,
            )
        selected_statuses = {
            item["status"]
            for item in status["steps"]
            if item["id"] in selected_steps
        }
        status["status"] = "succeeded" if selected_statuses == {"succeeded"} else "failed"
        status["active_step"] = None
        persist(status)
        print(f"Demo artifacts: {status_path}")
        return status
    except KeyboardInterrupt as error:
        status["status"] = "failed"
        status["errors"].append({"kind": "interrupted", "message": "Demo interrupted by user."})
        persist(status)
        raise error
    finally:
        if spark is not None:
            spark.stop()


def main() -> None:
    arguments = build_parser().parse_args()
    run_demo(
        arguments.config,
        mode=arguments.mode,
        step=arguments.step,
        hold_seconds=arguments.hold_seconds,
    )


if __name__ == "__main__":
    main()

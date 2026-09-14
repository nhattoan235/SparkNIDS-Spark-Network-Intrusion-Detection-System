"""Stable, small JSON contract shared by the Spark runner and Streamlit UI."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


DEMO_STEP_IDS = ("data", "parquet", "lazy", "execution", "cache", "mllib", "streaming")
VALID_STEP_STATUSES = frozenset({"pending", "running", "succeeded", "failed", "skipped"})
DEMO_STEP_METADATA = {
    "data": {"index": 1, "title": "Dữ liệu và schema"},
    "parquet": {"index": 2, "title": "CSV và Parquet"},
    "lazy": {"index": 3, "title": "Lazy evaluation"},
    "execution": {"index": 4, "title": "Job, Stage, Task"},
    "cache": {"index": 5, "title": "Cache"},
    "mllib": {"index": 6, "title": "MLlib Random Forest"},
    "streaming": {"index": 7, "title": "Structured Streaming"},
}


def _timestamp() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _find_step(status: dict[str, Any], step_id: str) -> dict[str, Any]:
    if step_id not in DEMO_STEP_IDS:
        raise ValueError(f"Unknown demo step: {step_id}")
    for step in status["steps"]:
        if step["id"] == step_id:
            return step
    raise ValueError(f"Demo status is missing step: {step_id}")


def new_demo_status(run_id: str, mode: str, spark_ui_url: str | None) -> dict[str, Any]:
    """Create the initial JSON-serializable status for one guided run."""
    return {
        "schema_version": 1,
        "run_id": str(run_id),
        "mode": str(mode),
        "status": "running",
        "active_step": None,
        "spark_ui_url": spark_ui_url,
        "started_at": _timestamp(),
        "updated_at": _timestamp(),
        "steps": [
            {
                "id": step_id,
                "index": DEMO_STEP_METADATA[step_id]["index"],
                "title": DEMO_STEP_METADATA[step_id]["title"],
                "status": "pending",
                "flow": {},
                "metrics": [],
                "explanation": "",
                "evidence": {},
            }
            for step_id in DEMO_STEP_IDS
        ],
        "errors": [],
    }


def set_step_running(status: dict[str, Any], step_id: str) -> None:
    step = _find_step(status, step_id)
    step["status"] = "running"
    status["active_step"] = step_id
    status["updated_at"] = _timestamp()


def set_step_result(status: dict[str, Any], step_id: str, result: dict[str, Any]) -> None:
    step = _find_step(status, step_id)
    step["status"] = "succeeded"
    step["result"] = result
    step["flow"] = result.get("flow", {})
    step["metrics"] = result.get("metrics", [])[:3]
    step["explanation"] = result.get("explanation", "")
    step["evidence"] = result.get("evidence", {})
    status["active_step"] = None
    status["updated_at"] = _timestamp()


def set_step_failed(
    status: dict[str, Any],
    step_id: str,
    error: BaseException,
    *,
    recovery_hint: str = "Kiểm tra log runner rồi chạy lại step này.",
) -> None:
    step = _find_step(status, step_id)
    error_payload = {
        "type": type(error).__name__,
        "message": str(error),
        "recovery_hint": recovery_hint,
    }
    step["status"] = "failed"
    step["error"] = error_payload
    status["errors"].append({"step_id": step_id, **error_payload})
    status["active_step"] = None
    status["updated_at"] = _timestamp()


def _validate_status_payload(payload: dict[str, Any]) -> None:
    status = payload.get("status")
    if status not in VALID_STEP_STATUSES | {"running"}:
        raise ValueError(f"Invalid demo status: {status}")
    for step in payload.get("steps", []):
        if step.get("status") not in VALID_STEP_STATUSES:
            raise ValueError(f"Invalid demo step status: {step.get('status')}")


def write_json_atomic(path: str | Path, payload: dict[str, Any]) -> Path:
    """Write a JSON payload via a same-directory temporary file and replace."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.name == "status.json" and "steps" in payload:
        _validate_status_payload(payload)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temporary.replace(target)
    return target


def write_latest_pointer(output_root: str | Path, status_path: str | Path) -> Path:
    root = Path(output_root).resolve()
    status = Path(status_path).resolve()
    try:
        relative_status = status.relative_to(root)
    except ValueError as error:
        raise ValueError("Status path must be inside the demo output root.") from error
    pointer = root / "latest.json"
    write_json_atomic(
        pointer,
        {"schema_version": 1, "status_path": relative_status.as_posix()},
    )
    return pointer

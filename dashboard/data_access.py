"""Small-artifact loading and validation used by the Streamlit dashboard."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.demo_contract import DEMO_STEP_IDS, VALID_STEP_STATUSES, new_demo_status


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "outputs" / "dashboard" / "manifest.json"
DEMO_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "demo"


def load_json(path: str | Path) -> Any:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    with resolved.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def load_dashboard_bundle(manifest_path: str | Path = MANIFEST_PATH) -> dict[str, Any]:
    manifest = load_json(manifest_path)
    required = {"eda", "model_comparison", "final_test_metrics", "benchmark", "alerts"}
    artifacts = manifest.get("artifacts", {})
    missing = sorted(required - set(artifacts))
    if missing:
        raise ValueError(f"Dashboard manifest is missing artifacts: {missing}")
    bundle = {name: load_json(path) for name, path in artifacts.items()}
    bundle["manifest"] = manifest
    return bundle


def _safe_demo_path(demo_root: Path, relative_path: str) -> Path:
    root = demo_root.resolve()
    candidate = (root / relative_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as error:
        raise ValueError("Status path must be inside demo output root.") from error
    return candidate


def _validate_demo_status(status: dict[str, Any]) -> None:
    if status.get("schema_version") != 1:
        raise ValueError(f"Unsupported demo status schema: {status.get('schema_version')}")
    if status.get("status") not in {"running", "succeeded", "failed"}:
        raise ValueError(f"Unsupported live demo status: {status.get('status')}")
    step_ids = [step.get("id") for step in status.get("steps", [])]
    if step_ids != list(DEMO_STEP_IDS):
        raise ValueError("Live demo status has an unexpected step order.")
    for step in status["steps"]:
        if step.get("status") not in VALID_STEP_STATUSES:
            raise ValueError(f"Unsupported step status: {step.get('status')}")


def _not_ready_status(message: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": None,
        "mode": None,
        "status": "not_ready",
        "fallback": False,
        "spark_ui_url": None,
        "steps": [
            {"id": step_id, "index": index, "title": step_id, "status": "pending", "flow": {}, "metrics": [], "explanation": "", "evidence": {}}
            for index, step_id in enumerate(DEMO_STEP_IDS, start=1)
        ],
        "errors": [{"message": message}],
    }


def load_demo_status(demo_root: str | Path = DEMO_OUTPUT_ROOT) -> dict[str, Any]:
    """Load the newest live status, falling back to existing offline artifacts."""
    root = Path(demo_root).resolve()
    latest_path = root / "latest.json"
    if not latest_path.exists():
        return load_static_demo_fallback()
    try:
        pointer = load_json(latest_path)
        if pointer.get("schema_version") != 1:
            raise ValueError(f"Unsupported demo pointer schema: {pointer.get('schema_version')}")
        relative_status = pointer.get("status_path")
        if not isinstance(relative_status, str) or not relative_status:
            raise ValueError("Demo pointer is missing status_path.")
        status_path = _safe_demo_path(root, relative_status)
        if not status_path.exists():
            return _not_ready_status("Demo runner is starting or replacing its status artifact.")
        status = load_json(status_path)
        _validate_demo_status(status)
        status["fallback"] = False
        return status
    except ValueError:
        raise
    except (FileNotFoundError, json.JSONDecodeError, OSError) as error:
        return _not_ready_status(str(error))


def load_static_demo_fallback() -> dict[str, Any]:
    """Build a small seven-step view from the already exported Phase artifacts."""
    status = new_demo_status("offline-fallback", "full", None)
    status["status"] = "succeeded"
    status["fallback"] = True
    status["explanation"] = "Runner chưa có artifact live; đang hiển thị số liệu đã export offline."
    try:
        bundle = load_dashboard_bundle()
    except (FileNotFoundError, ValueError, KeyError):
        bundle = {}

    def finish(step_id: str, *, flow: dict[str, str], metrics: list[dict[str, str]], explanation: str, evidence: dict[str, Any]) -> None:
        step = next(item for item in status["steps"] if item["id"] == step_id)
        step.update(status="succeeded", flow=flow, metrics=metrics[:3], explanation=explanation, evidence=evidence)

    eda = bundle.get("eda", {})
    classes = eda.get("class_distribution", [])
    total_rows = sum(int(item.get("flow_count", 0)) for item in classes)
    finish(
        "data",
        flow={"input": "Silver Parquet UNSW-NB15", "spark": "DataFrame + aggregate label", "result": "Dữ liệu sẵn sàng"},
        metrics=[{"label": "Network flow", "value": f"{total_rows:,}"}, {"label": "Schema", "value": "45 cột"}],
        explanation="Đây là snapshot offline của bước dữ liệu.",
        evidence={"source": "outputs/dashboard/eda_summary.json"},
    )
    benchmark = bundle.get("benchmark", {})
    formats = benchmark.get("benchmarks", {}).get("csv_vs_parquet", {})
    finish(
        "parquet",
        flow={"input": "CSV và Parquet", "spark": "Cùng một aggregate query", "result": "So sánh tốc độ"},
        metrics=[
            {"label": "Parquet speedup", "value": f"{float(formats.get('parquet_speedup_over_csv_median', 0.0)):.2f}x"},
            {"label": "Kết quả", "value": "Giống nhau"},
        ],
        explanation="Số liệu lấy từ benchmark Phase 8 đã chạy trước đó.",
        evidence={"source": "outputs/benchmarks/phase8_benchmark.json"},
    )
    for step_id, title in (("lazy", "Lazy evaluation"), ("execution", "Job/Stage/Task"), ("cache", "Cache")):
        finish(
            step_id,
            flow={"input": "DataFrame", "spark": title, "result": "Artifact offline"},
            metrics=[{"label": "Trạng thái", "value": "Đã export"}],
            explanation="Mở live runner để xem bằng chứng Spark cập nhật theo thời gian thực.",
            evidence={"source": "outputs/benchmarks/phase8_benchmark.json"},
        )
    model = bundle.get("model_comparison", {})
    candidates = model.get("candidates", [])
    selected = model.get("selection", {}).get("selected_candidate", "Random Forest")
    finish(
        "mllib",
        flow={"input": "Feature vector", "spark": "MLlib Pipeline + Random Forest", "result": "Normal/Attack"},
        metrics=[{"label": "Model", "value": str(selected)}, {"label": "Candidate", "value": str(len(candidates))}],
        explanation="Đây là kết quả model đã khóa trong đồ án, dùng để minh họa offline.",
        evidence={"source": "outputs/dashboard/model_comparison.json"},
    )
    finish(
        "streaming",
        flow={"input": "File Parquet mới", "spark": "Micro-batch + checkpoint", "result": "Artifact offline"},
        metrics=[{"label": "Trạng thái", "value": "Đã export"}],
        explanation="Live runner mới có thể cho xem tiến trình micro-batch và restart trực tiếp.",
        evidence={"source": "outputs/metrics/phase10_streaming.json"},
    )
    return status


def format_percent(value: float) -> str:
    return f"{100.0 * float(value):.2f}%"


def format_seconds(value: float) -> str:
    return f"{float(value):.3f}s"

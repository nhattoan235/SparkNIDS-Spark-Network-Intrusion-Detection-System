"""Small-artifact loading and validation used by the Streamlit dashboard."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = PROJECT_ROOT / "outputs" / "dashboard" / "manifest.json"


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


def format_percent(value: float) -> str:
    return f"{100.0 * float(value):.2f}%"


def format_seconds(value: float) -> str:
    return f"{float(value):.3f}s"

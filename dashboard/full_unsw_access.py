"""Read only small status/report artifacts for the full UNSW-NB15 page."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from src.spark_session import PROJECT_ROOT, load_config


def default_full_data_root() -> Path:
    configured = os.environ.get("SPARKNIDS_FULL_UNSW_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    config = load_config(PROJECT_ROOT / "configs" / "default.yaml")
    return Path(config["full_unsw"]["root"]).expanduser().resolve()


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"Expected an object in {path}")
    return value


def load_full_unsw_status(root: str | Path | None = None) -> dict[str, Any]:
    """Read the latest run pointer and its compact manifest, never the Parquet rows."""
    data_root = Path(root).expanduser().resolve() if root else default_full_data_root()
    pointer_path = data_root / "latest.json"
    if not pointer_path.is_file():
        return {"status": "missing", "ready": False, "run": {}, "root": str(data_root)}

    pointer = _read_json(pointer_path)
    if pointer.get("schema_version") != 1:
        raise ValueError("Unsupported full UNSW status pointer version.")
    run_path_value = pointer.get("run_path")
    if not isinstance(run_path_value, str) or not run_path_value:
        raise ValueError("Full UNSW status pointer has no run_path.")
    run_path = Path(run_path_value).expanduser()
    if not run_path.is_absolute():
        run_path = data_root / run_path
    run_path = run_path.resolve()
    try:
        run_path.relative_to(data_root)
    except ValueError as error:
        raise ValueError("Full UNSW run path must stay inside the full UNSW data root.") from error
    if not run_path.is_file():
        return {
            "status": "starting",
            "ready": False,
            "run": {},
            "root": str(data_root),
            "run_id": pointer.get("run_id"),
        }

    run = _read_json(run_path)
    if run.get("run_id") != pointer.get("run_id"):
        raise ValueError("Full UNSW status pointer and run manifest refer to different runs.")
    status = str(run.get("status", "unknown"))
    return {
        "status": status,
        "ready": status == "succeeded",
        "run": run,
        "root": str(data_root),
        "run_id": run["run_id"],
        "updated_at": run.get("updated_at"),
    }

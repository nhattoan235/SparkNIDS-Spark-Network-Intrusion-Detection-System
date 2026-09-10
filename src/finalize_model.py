"""Lock the Phase-6 winner as the final Phase-7 PipelineModel artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.ml import PipelineModel

from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def directory_sha256(path: Path) -> str:
    """Hash relative names and bytes for every file in a model directory."""
    if not path.is_dir():
        raise FileNotFoundError(f"Model directory does not exist: {path}")
    digest = hashlib.sha256()
    files = sorted(item for item in path.rglob("*") if item.is_file())
    if not files:
        raise ValueError(f"Model directory contains no files: {path}")
    for item in files:
        digest.update(item.relative_to(path).as_posix().encode("utf-8"))
        digest.update(b"\0")
        with item.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def finalize_model(config_path: str | Path | None = None, *, force: bool = False) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    final_config = config["final_model"]
    comparison_path = PROJECT_ROOT / config["paths"]["outputs"] / "metrics" / "phase6_model_comparison.json"
    comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
    selected_name = comparison["selection"]["selected_candidate"]
    selected_threshold = float(comparison["selection"]["selected_threshold"])
    if selected_name != final_config["source_candidate"]:
        raise ValueError("Final-model source candidate differs from the Phase-6 validation winner.")
    if abs(selected_threshold - float(final_config["decision_threshold"])) > 1e-12:
        raise ValueError("Locked threshold differs from the Phase-6 validation selection.")
    if comparison["selection"]["official_test_used"]:
        raise ValueError("Phase-6 selection unexpectedly reports official-test use.")

    selected = next(item for item in comparison["candidates"] if item["name"] == selected_name)
    source_path = PROJECT_ROOT / config["paths"]["models"] / "phase6_candidates" / selected_name
    final_path = PROJECT_ROOT / final_config["model_path"]
    metadata_path = PROJECT_ROOT / final_config["metadata_path"]
    if not force and (final_path.exists() or metadata_path.exists()):
        raise FileExistsError("Final model already exists; use --force only when intentionally replacing it before test evaluation.")

    spark = create_spark_session(config, app_name="unsw-nb15-phase7-finalize-model")
    try:
        candidate_model = PipelineModel.load(str(source_path))
        stage_types = [stage.__class__.__name__ for stage in candidate_model.stages]
        if not stage_types or stage_types[-1] != "RandomForestClassificationModel":
            raise ValueError(f"Unexpected final classifier stage: {stage_types}")
        classifier = candidate_model.stages[-1]
        expected_trees = int(selected["parameters"]["num_trees"])
        if int(classifier.getNumTrees) != expected_trees:
            raise ValueError("Loaded candidate tree count differs from the selected configuration.")
        candidate_model.write().overwrite().save(str(final_path))
    finally:
        spark.stop()

    model_files = [item for item in final_path.rglob("*") if item.is_file()]
    metadata = {
        "phase": 7,
        "status": "locked",
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "model_name": final_config["name"],
        "model_path": str(final_path),
        "pipeline_sha256": directory_sha256(final_path),
        "pipeline_file_count": len(model_files),
        "pipeline_size_bytes": sum(item.stat().st_size for item in model_files),
        "source_candidate": selected_name,
        "source_candidate_path": str(source_path),
        "source_selection_report": str(comparison_path),
        "source_selection_report_sha256": file_sha256(comparison_path),
        "selection_dataset": "validation only",
        "fit_dataset": "data/silver/modeling/train",
        "decision_threshold": selected_threshold,
        "validation_metrics_at_locked_threshold": selected["selected_threshold_metrics"],
        "classifier_parameters": selected["parameters"],
        "pipeline_stage_types": stage_types,
        "label_contract": {"0": "Normal", "1": "Attack", "positive_label": 1},
        "official_test_evaluated": False,
        "refit_after_selection": False,
        "refit_note": "The exact validation-selected candidate is locked; validation was not reused for fitting.",
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(metadata, indent=2, ensure_ascii=False))
    return metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lock the Phase-6 winner as the final PipelineModel.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    parser.add_argument("--force", action="store_true", help="Replace an existing final artifact before test evaluation.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    finalize_model(arguments.config, force=arguments.force)

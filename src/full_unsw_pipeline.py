"""Prepare the complete UNSW-NB15 dataset and train/evaluate one Spark MLlib IDS."""

from __future__ import annotations

import argparse
import copy
import json
import os
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.evaluate import binary_metrics_from_counts, select_threshold, threshold_metrics
from src.features import build_binary_pipeline
from src.full_unsw import (
    ZENODO_FILE_METADATA,
    ZENODO_RECORD_URL,
    audit_canonical_frame,
    audit_sources,
    canonicalize_raw,
    read_full_raw,
    split_full_data,
)
from src.preprocess import clean_dataframe
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.unsw_nb15_schema import EXPECTED_COLUMNS


def confusion_counts(predictions: DataFrame) -> dict[str, int]:
    """Return the four confusion-matrix counts without collecting prediction rows."""
    required = {"label", "prediction"}
    missing = sorted(required - set(predictions.columns))
    if missing:
        raise ValueError(f"Predictions are missing confusion-matrix columns: {missing}")
    row = predictions.agg(
        F.sum(F.when((F.col("label") == 1) & (F.col("prediction") == 1), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("label") == 0) & (F.col("prediction") == 1), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("label") == 0) & (F.col("prediction") == 0), 1).otherwise(0)).alias("tn"),
        F.sum(F.when((F.col("label") == 1) & (F.col("prediction") == 0), 1).otherwise(0)).alias("fn"),
    ).first()
    return {
        "true_positive": int(row.tp or 0),
        "false_positive": int(row.fp or 0),
        "true_negative": int(row.tn or 0),
        "false_negative": int(row.fn or 0),
    }


def source_count_audit(*, actual_rows: int, mirror_rows: int, published_rows: int) -> dict[str, int]:
    """Require the hash-verified mirror's row count while recording UNSW's published-count difference."""
    if int(actual_rows) != int(mirror_rows):
        raise ValueError(
            f"Actual source row count {actual_rows:,} does not match the verified mirror count {mirror_rows:,}."
        )
    return {
        "actual_rows": int(actual_rows),
        "verified_mirror_expected_rows": int(mirror_rows),
        "published_rows": int(published_rows),
        "difference_from_published": int(actual_rows) - int(published_rows),
    }


def resolve_full_root(config: dict[str, Any], override: str | Path | None = None) -> Path:
    """Resolve the single data root; an environment override supports other laptops."""
    selected = override or os.environ.get("SPARKNIDS_FULL_UNSW_ROOT") or config.get("full_unsw", {}).get("root")
    if not selected:
        raise ValueError("Set full_unsw.root in configs/default.yaml or SPARKNIDS_FULL_UNSW_ROOT.")
    return Path(selected).expanduser().resolve()


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return value


def _json_for_console(payload: dict[str, Any], *, encoding: str | None = None) -> str:
    """Keep readable Unicode when the console supports it; escape otherwise."""
    readable = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        readable.encode(encoding or getattr(sys.stdout, "encoding", None) or "utf-8")
    except (LookupError, UnicodeEncodeError):
        return json.dumps(payload, ensure_ascii=True, indent=2)
    return readable


@contextmanager
def _pipeline_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".full_unsw_pipeline.lock"
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        raise RuntimeError(f"A full UNSW Spark job already holds the lock: {lock}") from error
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(f"pid={os.getpid()} started={_now()}\n")
        yield
    finally:
        lock.unlink(missing_ok=True)


def _spark_for_full_data(config: dict[str, Any], root: Path, app_name: str) -> SparkSession:
    resolved = copy.deepcopy(config)
    settings = resolved["spark"].setdefault("settings", {})
    settings["spark.local.dir"] = str(root / "tmp" / "spark")
    return create_spark_session(resolved, app_name=app_name)


def _new_run_id() -> str:
    return f"unsw-{datetime.now().astimezone():%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:6]}"


def _set_run_state(root: Path, run_dir: Path, run: dict[str, Any], state: str) -> None:
    run["status"] = state
    run["updated_at"] = _now()
    _write_json_atomic(run_dir / "run.json", run)
    _write_json_atomic(
        root / "latest.json",
        {"schema_version": 1, "run_id": run["run_id"], "status": state, "run_path": str(run_dir / "run.json")},
    )


def _label_distribution(dataframe: DataFrame) -> dict[str, int]:
    return {
        str(row["label"]): int(row["count"])
        for row in dataframe.groupBy("label").count().orderBy("label").collect()
    }


def _spark_evidence(spark: SparkSession, group_id: str) -> dict[str, int]:
    tracker = spark.sparkContext.statusTracker()
    job_ids = list(tracker.getJobIdsForGroup(group_id))
    stage_ids: set[int] = set()
    for job_id in job_ids:
        info = tracker.getJobInfo(job_id)
        if info is not None:
            stage_ids.update(int(stage_id) for stage_id in info.stageIds)
    stages = []
    for stage_id in stage_ids:
        info = tracker.getStageInfo(stage_id)
        if info is not None:
            stages.append(
                {
                    "stage_id": stage_id,
                    "tasks_created": int(info.numTasks),
                    "tasks_completed": int(info.numCompletedTasks),
                    "tasks_failed": int(info.numFailedTasks),
                }
            )
    return {
        "job_count": len(job_ids),
        "stage_count": len(stages),
        "stages_with_completed_tasks": sum(1 for stage in stages if stage["tasks_completed"]),
        "tasks_created": sum(stage["tasks_created"] for stage in stages),
        "tasks_completed": sum(stage["tasks_completed"] for stage in stages),
        "tasks_failed": sum(stage["tasks_failed"] for stage in stages),
    }


def _save_splits(spark: SparkSession, silver: DataFrame, run_dir: Path, seed: int) -> dict[str, Any]:
    splits = split_full_data(silver, seed=seed)
    result: dict[str, Any] = {}
    for name, frame in splits.items():
        destination = run_dir / "splits" / name
        count = frame.count()
        if count == 0:
            raise ValueError(f"Full UNSW split is empty: {name}")
        frame.write.mode("errorifexists").option("compression", "snappy").parquet(str(destination))
        persisted = spark.read.parquet(str(destination))
        persisted_count = persisted.count()
        if persisted_count != count:
            raise ValueError(f"Persisted {name} row count changed: {count} -> {persisted_count}")
        result[name] = {
            "rows": count,
            "label_counts": _label_distribution(persisted),
            "path": str(destination),
        }
    if sum(item["rows"] for item in result.values()) != silver.count():
        raise ValueError("Train/validation/test row counts do not equal cleaned Silver rows.")
    return result


def prepare_full_dataset(
    *, config_path: str | Path | None = None, root_override: str | Path | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """Create a provenance-checked Bronze/Silver dataset and deterministic 70/15/15 splits."""
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    root = resolve_full_root(config, root_override)
    source_dir = root / "raw"
    run_id = run_id or _new_run_id()
    run_dir = root / "runs" / run_id
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing full UNSW run: {run_dir}")

    with _pipeline_lock(root):
        run_dir.mkdir(parents=True)
        run: dict[str, Any] = {
            "schema_version": 1,
            "run_id": run_id,
            "status": "preparing",
            "phase": "prepare",
            "started_at": _now(),
            "data_root": str(root),
            "run_dir": str(run_dir),
            "source_url": ZENODO_RECORD_URL,
        }
        _set_run_state(root, run_dir, run, "preparing")
        spark = None
        try:
            source_manifest = audit_sources(source_dir, expected_metadata=ZENODO_FILE_METADATA)
            run["source_manifest"] = source_manifest
            full_settings = config.get("full_unsw", {})
            expected_rows = int(full_settings.get("expected_rows", 0))
            published_rows = int(full_settings.get("published_rows", 0))
            spark = _spark_for_full_data(config, root, "unsw-nb15-full-prepare")
            group_id = f"{run_id}-prepare"
            spark.sparkContext.setJobGroup(group_id, "Read, validate, clean and split all UNSW-NB15 rows", True)
            started = time.perf_counter()

            raw = read_full_raw(spark, source_dir)
            bronze_path = run_dir / "bronze"
            canonicalize_raw(raw).write.mode("errorifexists").option("compression", "snappy").parquet(str(bronze_path))
            bronze = spark.read.parquet(str(bronze_path))
            quality_before = audit_canonical_frame(bronze)
            row_audit = source_count_audit(
                actual_rows=quality_before["rows"],
                mirror_rows=expected_rows or quality_before["rows"],
                published_rows=published_rows or quality_before["rows"],
            )
            if quality_before["invalid_label_rows"]:
                raise ValueError(f"Found {quality_before['invalid_label_rows']:,} rows with invalid binary labels.")

            conflicting = (
                bronze.groupBy("flow_fingerprint")
                .agg(F.countDistinct("label").alias("_labels"))
                .filter(F.col("_labels") > 1)
                .select("flow_fingerprint")
            )
            unambiguous = bronze.join(conflicting, on="flow_fingerprint", how="left_anti")
            cleaned = clean_dataframe(unambiguous)
            metadata = unambiguous.select("id", "flow_fingerprint", "source_file")
            silver = cleaned.join(metadata, on="id", how="inner").select(
                *EXPECTED_COLUMNS, "flow_fingerprint", "source_file"
            )
            silver_path = run_dir / "silver"
            silver.write.mode("errorifexists").option("compression", "snappy").parquet(str(silver_path))
            silver = spark.read.parquet(str(silver_path))
            quality_after = audit_canonical_frame(silver)
            if quality_after["conflicting_fingerprint_groups"]:
                raise ValueError("Conflicting flow labels remain after full-data cleaning.")

            splits = _save_splits(spark, silver, run_dir, seed=int(config["project"]["seed"]))
            run.update(
                {
                    "phase": "prepare",
                    "status": "prepared",
                    "spark_version": spark.version,
                    "spark_master": spark.sparkContext.master,
                    "expected_source_rows_for_verified_mirror": expected_rows or None,
                    "published_source_rows": published_rows or None,
                    "source_vs_published_row_difference": row_audit["difference_from_published"],
                    "source_row_count_audit": row_audit,
                    "bronze_path": str(bronze_path),
                    "silver_path": str(silver_path),
                    "quality_before_cleaning": quality_before,
                    "quality_after_cleaning": quality_after,
                    "rows_removed": quality_before["rows"] - quality_after["rows"],
                    "duplicate_rows_retained": quality_after["duplicate_rows"],
                    "duplicate_rows_removed": quality_before["duplicate_rows"] - quality_after["duplicate_rows"],
                    "splits": splits,
                    "prepare_seconds": time.perf_counter() - started,
                    "spark_evidence": _spark_evidence(spark, group_id),
                }
            )
            _set_run_state(root, run_dir, run, "prepared")
            return run
        except Exception as error:
            run["error"] = str(error)
            run["failed_at"] = _now()
            _set_run_state(root, run_dir, run, "failed")
            raise
        finally:
            if spark is not None:
                spark.stop()


def _read_run(root: Path, run_id: str | None = None) -> tuple[Path, dict[str, Any]]:
    if run_id:
        run_dir = root / "runs" / run_id
    else:
        latest = _read_json(root / "latest.json")
        run_dir = Path(latest["run_path"]).parent
    run = _read_json(run_dir / "run.json")
    if run.get("run_id") != run_dir.name:
        raise ValueError("Full UNSW run ID does not match its folder.")
    return run_dir, run


def _sample_outcomes(scored: DataFrame, outcome: str, limit: int = 3) -> list[dict[str, Any]]:
    conditions = {
        "bắt đúng tấn công": (F.col("label") == 1) & (F.col("prediction") == 1),
        "báo nhầm": (F.col("label") == 0) & (F.col("prediction") == 1),
        "bỏ sót": (F.col("label") == 1) & (F.col("prediction") == 0),
        "nhận đúng bình thường": (F.col("label") == 0) & (F.col("prediction") == 0),
    }
    selected = scored.filter(conditions[outcome]).select(
        "id", "source_file", "proto", "service", "attack_cat", "label", "prediction", "attack_score"
    ).limit(limit)
    return [row.asDict(recursive=True) | {"outcome": outcome} for row in selected.collect()]


def train_and_evaluate_full_data(
    *, config_path: str | Path | None = None, root_override: str | Path | None = None, run_id: str | None = None
) -> dict[str, Any]:
    """Fit on the complete train split, lock threshold on validation, then score independent test."""
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    root = resolve_full_root(config, root_override)

    with _pipeline_lock(root):
        run_dir, run = _read_run(root, run_id)
        if run.get("status") != "prepared":
            raise ValueError(f"Expected a prepared run; current status is {run.get('status')}.")
        spark = _spark_for_full_data(config, root, "unsw-nb15-full-mllib")
        run["phase"] = "train"
        _set_run_state(root, run_dir, run, "training")
        try:
            model_config = config.get("full_unsw", {})
            training = spark.read.parquet(str(run_dir / "splits" / "train"))
            validation = spark.read.parquet(str(run_dir / "splits" / "validation"))
            started = time.perf_counter()
            group_id = f"{run['run_id']}-fit"
            spark.sparkContext.setJobGroup(group_id, "Fit Spark MLlib IDS on the full train split", True)
            classifier = LogisticRegression(
                labelCol="label",
                featuresCol=config["modeling"]["features_col"],
                maxIter=int(model_config.get("max_iter", 25)),
                regParam=float(config["baseline_logistic_regression"]["reg_param"]),
                elasticNetParam=float(config["baseline_logistic_regression"]["elastic_net_param"]),
                standardization=False,
            )
            pipeline = build_binary_pipeline(config, classifier=classifier)
            pipeline_model = pipeline.fit(training)
            fit_seconds = time.perf_counter() - started

            model_path = run_dir / "model" / "pipeline"
            pipeline_model.write().save(str(model_path))
            validation_predictions = pipeline_model.transform(validation).cache()
            validation_rows = validation_predictions.count()
            if validation_rows != run["splits"]["validation"]["rows"]:
                raise ValueError("MLlib validation output row count does not match its input.")
            thresholds = model_config.get("thresholds", config["baseline_logistic_regression"]["threshold_candidates"])
            curve = threshold_metrics(validation_predictions, thresholds)
            selected = select_threshold(curve, metric=str(model_config.get("validation_metric", "f1")))
            validation_predictions.unpersist()

            locked = {
                "schema_version": 1,
                "run_id": run["run_id"],
                "model_locked_at": _now(),
                "model": "Spark MLlib LogisticRegression",
                "fit_rows": run["splits"]["train"]["rows"],
                "validation_rows": validation_rows,
                "fit_seconds": fit_seconds,
                "decision_threshold": selected["threshold"],
                "threshold_selected_on": "validation only",
                "threshold_metric": model_config.get("validation_metric", "f1"),
                "selected_validation_metrics": selected,
                "source_sha256": {item["name"]: item["sha256"] for item in run["source_manifest"]["files"]},
                "spark_version": spark.version,
                "spark_master": spark.sparkContext.master,
                "model_path": str(model_path),
                "features_excluded": ["id", "attack_cat", "label", "source_file", "flow_fingerprint"],
            }
            _write_json_atomic(run_dir / "model" / "model_lock.json", locked)

            test = spark.read.parquet(str(run_dir / "splits" / "test"))
            test_count = run["splits"]["test"]["rows"]
            score_group = f"{run['run_id']}-score"
            spark.sparkContext.setJobGroup(score_group, "Score every independent UNSW-NB15 test flow", True)
            score_started = time.perf_counter()
            scored = (
                pipeline_model.transform(test)
                .withColumn("attack_score", vector_to_array(F.col("probability"))[1])
                .withColumn(
                    "prediction",
                    (F.col("attack_score") >= F.lit(float(selected["threshold"]))).cast("int"),
                )
            ).persist(StorageLevel.DISK_ONLY)
            predictions_path = run_dir / "predictions" / "test"
            try:
                prediction_rows = scored.count()
                if prediction_rows != test_count:
                    raise ValueError(
                        f"Independent test row mismatch: expected={test_count}, predictions={prediction_rows}."
                    )
                scored.select(
                    "id", "source_file", "attack_cat", "label", "prediction", "attack_score", "proto", "service"
                ).write.mode("errorifexists").option("compression", "snappy").parquet(str(predictions_path))
                counts = confusion_counts(scored)
                metrics = binary_metrics_from_counts(**{
                    "tp": counts["true_positive"],
                    "fp": counts["false_positive"],
                    "tn": counts["true_negative"],
                    "fn": counts["false_negative"],
                })
                if metrics["total"] != test_count:
                    raise ValueError(
                        f"Independent test confusion mismatch: test={test_count}, confusion={metrics['total']}."
                    )
                examples = [
                    item
                    for outcome in ("bắt đúng tấn công", "báo nhầm", "bỏ sót", "nhận đúng bình thường")
                    for item in _sample_outcomes(scored, outcome)
                ]
            finally:
                scored.unpersist()
            score_seconds = time.perf_counter() - score_started
            test_evidence = _spark_evidence(spark, score_group)
            report = {
                **run,
                "status": "succeeded",
                "phase": "complete",
                "completed_at": _now(),
                "model_lock": locked,
                "test": {
                    "rows": test_count,
                    "prediction_rows": prediction_rows,
                    "decision_threshold": selected["threshold"],
                    "metrics": metrics,
                    "score_seconds": score_seconds,
                    "spark_evidence": test_evidence,
                    "predictions_path": str(predictions_path),
                    "examples": examples,
                    "evaluation_protocol": "Independent fingerprint split; model and threshold locked before test scoring.",
                },
            }
            _write_json_atomic(run_dir / "report.json", report)
            _set_run_state(root, run_dir, report, "succeeded")
            return report
        except Exception as error:
            run["error"] = str(error)
            run["failed_at"] = _now()
            _set_run_state(root, run_dir, run, "failed")
            raise
        finally:
            spark.stop()


def run_all(*, config_path: str | Path | None = None, root_override: str | Path | None = None) -> dict[str, Any]:
    prepared = prepare_full_dataset(config_path=config_path, root_override=root_override)
    return train_and_evaluate_full_data(
        config_path=config_path,
        root_override=root_override,
        run_id=prepared["run_id"],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full-data UNSW-NB15 Spark IDS pipeline.")
    parser.add_argument("--phase", choices=("prepare", "train", "all"), default="all")
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--root", type=Path, default=None)
    parser.add_argument("--run-id", default=None, help="Existing prepared run ID for --phase train.")
    arguments = parser.parse_args()
    if arguments.phase == "prepare":
        result = prepare_full_dataset(config_path=arguments.config, root_override=arguments.root, run_id=arguments.run_id)
    elif arguments.phase == "train":
        result = train_and_evaluate_full_data(
            config_path=arguments.config, root_override=arguments.root, run_id=arguments.run_id
        )
    else:
        if arguments.run_id:
            parser.error("--run-id is only valid with --phase train or prepare.")
        result = run_all(config_path=arguments.config, root_override=arguments.root)
    print(_json_for_console(result))


if __name__ == "__main__":
    main()

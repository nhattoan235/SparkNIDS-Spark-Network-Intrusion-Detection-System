"""Train a compact Spark MLlib IDS model on features observable in the lab."""

from __future__ import annotations

import argparse
import json
import hashlib
import math
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from src.evaluate import threshold_metrics, select_threshold
from src.features import split_train_validation
from src.lab_features import FLOW_SCHEMA, SESSION_FEATURES, aggregate_sessions
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


LAB_FEATURES = ("dur", "sbytes", "dbytes", "proto", "service")
MODEL_DIR = PROJECT_ROOT / "models" / "live_lab"


def build_lab_pipeline(*, num_trees: int = 20, max_depth: int = 6) -> Pipeline:
    stages = [
        StringIndexer(inputCol=column, outputCol=f"{column}_index", handleInvalid="keep")
        for column in ("proto", "service")
    ]
    stages.append(
        OneHotEncoder(
            inputCols=["proto_index", "service_index"],
            outputCols=["proto_ohe", "service_ohe"],
            handleInvalid="keep",
        )
    )
    stages.append(
        VectorAssembler(
            inputCols=["dur", "sbytes", "dbytes", "proto_ohe", "service_ohe"],
            outputCol="features",
            handleInvalid="error",
        )
    )
    stages.append(
        RandomForestClassifier(
            labelCol="label", featuresCol="features", numTrees=num_trees,
            maxDepth=max_depth, maxBins=32, seed=42,
        )
    )
    return Pipeline(stages=stages)


def build_session_pipeline(*, num_trees: int = 40, max_depth: int = 5) -> Pipeline:
    return Pipeline(
        stages=[
            VectorAssembler(
                inputCols=list(SESSION_FEATURES),
                outputCol="features",
                handleInvalid="error",
            ),
            RandomForestClassifier(
                labelCol="label",
                featuresCol="features",
                numTrees=num_trees,
                maxDepth=max_depth,
                seed=42,
                minInstancesPerNode=2,
            ),
        ]
    )


def stratified_run_split(rows: list[tuple[str, int]]) -> dict[str, list[str]]:
    """Split whole traffic runs by label so events from one run cannot cross splits."""
    buckets: dict[int, list[str]] = {0: [], 1: []}
    for run_id, label in rows:
        if int(label) not in buckets:
            raise ValueError(f"Unexpected lab label: {label}")
        buckets[int(label)].append(str(run_id))

    result = {"train": [], "validation": [], "test": []}
    for label, run_ids in buckets.items():
        ordered = sorted(run_ids, key=lambda value: hashlib.sha256(value.encode()).hexdigest())
        if len(ordered) < 5:
            raise ValueError(
                "Live Lab needs at least five complete training runs for each label. "
                "Run `docker compose -f lab/compose.yaml run --rm generator training` first."
            )
        train_end = max(1, math.floor(len(ordered) * 0.60))
        validation_end = max(train_end + 1, math.floor(len(ordered) * 0.80))
        if validation_end >= len(ordered):
            validation_end = len(ordered) - 1
        result["train"].extend(ordered[:train_end])
        result["validation"].extend(ordered[train_end:validation_end])
        result["test"].extend(ordered[validation_end:])
    return result


def select_session_threshold(curve: list[dict]) -> dict:
    """Prefer sensitivity when validation quality is identical."""
    best = select_threshold(curve, metric="f1")
    quality = (best["f1"], best["recall"], -best["false_positive_rate"])
    return min(
        (
            row for row in curve
            if (row["f1"], row["recall"], -row["false_positive_rate"]) == quality
        ),
        key=lambda row: row["threshold"],
    )


def select_session_training_mode(run_rows: list[tuple[str, int, str]]) -> str:
    calibration_counts = {label: 0 for label in (0, 1)}
    for _, label, lab_mode in run_rows:
        if lab_mode == "calibration" and label in calibration_counts:
            calibration_counts[label] += 1
    if all(count >= 5 for count in calibration_counts.values()):
        return "calibration"
    return "training"


def load_complete_lab_sessions(spark, *, incoming: Path, run_truth: Path):
    incoming = Path(incoming)
    flow_paths = [
        str(incoming / mode)
        for mode in ("training", "calibration")
        if (incoming / mode).is_dir()
    ] or [str(incoming)]
    flows = (
        spark.read.schema(FLOW_SCHEMA).json(flow_paths)
        .where(
            F.col("lab_mode").isin("training", "calibration")
            & (F.col("record_type") == "flow")
        )
    )
    truth = (
        spark.read.json(str(run_truth))
        .where(F.col("lab_mode").isin("training", "calibration"))
        .select(
            "run_id",
            F.col("label").cast("int").alias("label"),
            "expected_flows",
            "lab_mode",
        )
    )
    sessions = aggregate_sessions(flows).join(truth, "run_id", "inner")
    return sessions.where(F.col("flow_count") == F.col("expected_flows")).drop("expected_flows")


def evaluate_challenge_runs(predictions, *, threshold: float) -> dict | None:
    """Score complete, older Docker runs excluded from calibration fitting."""
    challenge = predictions.where(F.col("lab_mode") == "training").drop("lab_mode")
    if not challenge.take(1):
        return None
    return {
        "source_mode": "training",
        **threshold_metrics(challenge, [threshold])[0],
    }


def train_session_model(spark, *, incoming: Path, run_truth: Path) -> dict:
    all_sessions = load_complete_lab_sessions(spark, incoming=incoming, run_truth=run_truth)
    run_rows_by_mode = [
        (row.run_id, int(row.label), row.lab_mode)
        for row in all_sessions.select("run_id", "label", "lab_mode").collect()
    ]
    training_mode = select_session_training_mode(run_rows_by_mode)
    sessions = all_sessions.where(F.col("lab_mode") == training_mode).drop("lab_mode")
    run_rows = [
        (run_id, label)
        for run_id, label, mode in run_rows_by_mode
        if mode == training_mode
    ]
    split_ids = stratified_run_split(run_rows)
    training = sessions.where(F.col("run_id").isin(split_ids["train"]))
    validation = sessions.where(F.col("run_id").isin(split_ids["validation"]))
    test = sessions.where(F.col("run_id").isin(split_ids["test"]))

    model = build_session_pipeline().fit(training)
    validation_curve = threshold_metrics(model.transform(validation), (0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8))
    selected = select_session_threshold(validation_curve)
    test_metrics = threshold_metrics(model.transform(test), [selected["threshold"]])[0]
    model.write().overwrite().save(str(MODEL_DIR / "session_pipeline"))
    report = {
        "features": list(SESSION_FEATURES),
        "training_mode": training_mode,
        "split_method": "Stratified by complete run_id: 60% train, 20% validation, 20% test",
        "training_runs": len(split_ids["train"]),
        "validation_runs": len(split_ids["validation"]),
        "test_runs": len(split_ids["test"]),
        "threshold": selected["threshold"],
        "validation": selected,
        "test": test_metrics,
        "note": "Controlled Docker scenarios; metrics describe this lab distribution only.",
    }
    if training_mode == "calibration":
        challenge_scores = model.transform(all_sessions.where(F.col("lab_mode") == "training"))
        challenge = evaluate_challenge_runs(challenge_scores, threshold=selected["threshold"])
        if challenge:
            report["challenge_test"] = challenge
    return report


def audit_session_challenge() -> dict:
    """Refresh the challenge report with the saved model, without retraining it."""
    report_path = MODEL_DIR / "metrics.json"
    if not report_path.is_file():
        raise FileNotFoundError("Train the Docker session model before auditing it.")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    session_report = report["lab_sessions"]
    if session_report["training_mode"] != "calibration":
        raise ValueError("Challenge audit requires a model trained on calibration runs.")
    spark = create_spark_session(load_config(), app_name="spark-ids-live-lab-audit")
    try:
        sessions = load_complete_lab_sessions(
            spark,
            incoming=PROJECT_ROOT / "data" / "lab" / "incoming",
            run_truth=PROJECT_ROOT / "data" / "lab" / "truth" / "runs",
        )
        model = PipelineModel.load(str(MODEL_DIR / "session_pipeline"))
        challenge_scores = model.transform(sessions.where(F.col("lab_mode") == "training"))
        challenge = evaluate_challenge_runs(challenge_scores, threshold=session_report["threshold"])
        if not challenge:
            raise ValueError("No complete training-mode Docker runs are available for the challenge audit.")
        session_report["challenge_test"] = challenge
        temporary = report_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
        temporary.replace(report_path)
        return challenge
    finally:
        spark.stop()


def train_lab_model() -> dict:
    config = load_config()
    spark = create_spark_session(config, app_name="spark-ids-live-lab-train")
    try:
        silver = PROJECT_ROOT / config["paths"]["silver"]
        source = spark.read.parquet(str(silver / "train")).select("id", "label", *LAB_FEATURES)
        training, validation = split_train_validation(
            source,
            validation_fraction=float(config["modeling"]["validation_fraction"]),
            seed=int(config["project"]["seed"]),
        )
        model = build_lab_pipeline().fit(training)
        validation_scores = model.transform(validation)
        selected = select_threshold(
            threshold_metrics(validation_scores, (0.3, 0.4, 0.5, 0.6, 0.7)), metric="f1"
        )
        threshold = selected["threshold"]
        official_test = spark.read.parquet(str(silver / "test")).select("label", *LAB_FEATURES)
        test_metrics = threshold_metrics(model.transform(official_test), [threshold])[0]
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model.write().overwrite().save(str(MODEL_DIR / "pipeline"))
        session_report = train_session_model(
            spark,
            incoming=PROJECT_ROOT / "data" / "lab" / "incoming",
            run_truth=PROJECT_ROOT / "data" / "lab" / "truth" / "runs",
        )
        report = {
            "features": list(LAB_FEATURES),
            "training_rows": training.count(),
            "validation_rows": validation.count(),
            "official_test_rows": official_test.count(),
            "threshold": threshold,
            "validation": selected,
            "official_test": test_metrics,
            "lab_sessions": session_report,
            "note": "Lab traffic has a different distribution and is evaluated separately.",
        }
        (MODEL_DIR / "metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        spark.stop()


def train_session_only_model() -> dict:
    config = load_config()
    spark = create_spark_session(config, app_name="spark-ids-live-lab-train")
    try:
        report_path = MODEL_DIR / "metrics.json"
        report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.is_file() else {}
        report["lab_sessions"] = train_session_model(
            spark,
            incoming=PROJECT_ROOT / "data" / "lab" / "incoming",
            run_truth=PROJECT_ROOT / "data" / "lab" / "truth" / "runs",
        )
        report["note"] = "Live Lab uses isolated Docker calibration; UNSW-NB15 metrics remain separate."
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report["lab_sessions"]
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--session-only",
        action="store_true",
        help="Train only the Docker Live Lab model; do not read UNSW-NB15 Parquet files.",
    )
    mode.add_argument(
        "--audit-only",
        action="store_true",
        help="Evaluate the saved Docker model on complete, unseen training-mode runs.",
    )
    args = parser.parse_args()
    result = (
        audit_session_challenge()
        if args.audit_only
        else train_session_only_model() if args.session_only else train_lab_model()
    )
    print(json.dumps(result, indent=2))

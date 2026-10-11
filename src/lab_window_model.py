"""Train and check observed-IP windows from independently collected Docker experiments."""

from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import functions as F

from dashboard.live_lab_controller import LiveLabController
from src.evaluate import select_threshold, threshold_metrics
from src.lab_features import FLOW_SCHEMA, WINDOW_SECONDS, aggregate_windows
from src.spark_session import PROJECT_ROOT, create_spark_session

MODEL_ROOT = PROJECT_ROOT / "models/live_lab"
MODEL_FEATURES = ("flow_rate", "port_scan_ratio", "http_ratio")


def atomic_report(path, record):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(record, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def collect_experiments(duration: int = 50) -> dict:
    """Each split uses distinct container runs and seeds, never windows from one run."""
    controller = LiveLabController(PROJECT_ROOT)
    splits = {}
    collection_path = MODEL_ROOT / "window_collection.json"
    built = False
    status_path = PROJECT_ROOT / "outputs/live_lab/preparation.json"
    try:
        for index, split in enumerate(("train", "validation", "test")):
            if collection_path.is_file():
                try:
                    splits.update(json.loads(collection_path.read_text(encoding="utf-8")).get("splits", {}))
                except (OSError, ValueError):
                    pass
            if split not in splits:
                candidates = []
                for path in controller.experiments_dir.glob("*.json"):
                    try:
                        candidate = json.loads(path.read_text(encoding="utf-8"))
                    except (OSError, ValueError):
                        continue
                    if (candidate.get("kind") == "live" and candidate.get("lab_mode") == f"window_{split}"
                        and candidate.get("status") == "completed"
                        and len(candidate.get("senders", [])) == 15
                        and all(row.get("status") == "Hoàn tất" and row.get("failed", 0) == 0 for row in candidate["senders"])):
                        candidates.append(candidate)
                if candidates:
                    candidate = max(candidates, key=lambda item: item.get("started_at", ""))
                    splits[split] = candidate["experiment_id"]
            atomic_report(status_path, {"status": "collecting", "split": split, "updated_at": time.time()})
            if split not in splits:
                experiment_id = controller.start_live(duration=duration, seed=(173, 941, 2027)[index],
                    lab_mode=f"window_{split}", build=not built)
                built = True
                splits[split] = experiment_id
                print(f"Collecting {split}: {experiment_id}", flush=True)
                while controller.active:
                    time.sleep(2)
                    controller.status(experiment_id)
                manifest = controller.status(experiment_id)
                if manifest["status"] != "completed" or any(row.get("failed", 0) for row in manifest["senders"]):
                    raise RuntimeError(f"Incomplete Docker collection: {split}; inspect sender logs.")
                time.sleep(2)
            atomic_report(collection_path, {"splits": splits, "duration": duration,
                "created_at": datetime.now(timezone.utc).isoformat()})
        return splits
    finally:
        if controller.active:
            controller.stop()


def labeled_windows(spark, split: str, experiment_id: str):
    flows = spark.read.schema(FLOW_SCHEMA).json(str(PROJECT_ROOT / f"data/lab/incoming/window_{split}"))
    flows = flows.where(F.col("experiment_id") == experiment_id).dropDuplicates(["event_id"])
    truth = spark.read.schema("event_id string, label int").json(
        str(PROJECT_ROOT / f"data/lab/truth/events/{experiment_id}")
    )
    joined = flows.join(truth, "event_id", "inner")
    labels = joined.withColumn("event_time", F.to_timestamp("timestamp")).groupBy(
        "experiment_id", "source_ip", F.window("event_time", f"{WINDOW_SECONDS} seconds")
    ).agg(F.max("label").alias("label"), F.count("*").alias("labeled_count"),
        F.countDistinct("label").alias("label_count"))
    labels = labels.select("experiment_id", "source_ip", F.col("window.start").alias("window_start"),
        "label", "labeled_count", "label_count")
    last_event = flows.agg(F.max(F.to_timestamp("timestamp")).alias("latest")).first().latest
    return aggregate_windows(flows).join(labels, ["experiment_id", "source_ip", "window_start"]).where(
        (F.col("window_end") <= F.lit(last_event)) & (F.col("flow_count") == F.col("labeled_count"))
        & (F.col("label_count") == 1)
    ).drop("labeled_count")


def rule_score(frame):
    """An explicitly documented baseline, not a hidden substitute for MLlib."""
    return frame.withColumn("rule_score", F.when(
        (F.col("port_scan_ratio") >= 0.5) | (F.col("flow_rate") >= 10), 1.0
    ).otherwise(0.0))


def train_windows() -> dict:
    collection = json.loads((MODEL_ROOT / "window_collection.json").read_text(encoding="utf-8"))
    splits = collection["splits"]
    if len(set(splits.values())) != 3:
        raise ValueError("Train, validation and test must come from separate experiments")
    atomic_report(PROJECT_ROOT / "outputs/live_lab/preparation.json", {"status": "training", "updated_at": time.time()})
    spark = create_spark_session(app_name="live-ip-window-model")
    frames = {}
    try:
        for name, experiment in splits.items():
            frame = labeled_windows(spark, name, experiment).cache()
            counts = {int(row.label): int(row["count"]) for row in frame.groupBy("label").count().collect()}
            if any(counts.get(label, 0) < 5 for label in (0, 1)):
                raise ValueError(f"Not enough labeled complete windows in {name}: {counts}")
            frames[name] = frame
        pipeline = Pipeline(stages=[VectorAssembler(inputCols=list(MODEL_FEATURES), outputCol="features"),
            RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=30, maxDepth=5,
                minInstancesPerNode=2, seed=42)])
        model = pipeline.fit(frames["train"])
        candidates = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
        selected = select_threshold(threshold_metrics(model.transform(frames["validation"]), candidates))
        test = threshold_metrics(model.transform(frames["test"]), [selected["threshold"]])[0]
        baseline = threshold_metrics(rule_score(frames["test"]), [0.5], score_col="rule_score")[0]
        model.write().overwrite().save(str(MODEL_ROOT / "window_pipeline"))
        report = {"version": 1, "window_seconds": WINDOW_SECONDS, "features": list(MODEL_FEATURES),
            "threshold": selected["threshold"], "validation": selected, "test": test, "rule_test": baseline,
            "split_experiments": splits, "rows": {name: frame.count() for name, frame in frames.items()},
            "rule": "At least half of flows target decoy ports OR at least 10 connections/second",
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "note": "Actual isolated Docker traffic; independent experiments, varied profiles. Mixed windows use max event label; only complete, fully labeled windows evaluated. This is not accuracy on production networks."}
        atomic_report(MODEL_ROOT / "window_metrics.json", report)
        atomic_report(PROJECT_ROOT / "outputs/live_lab/preparation.json", {"status": "ready", "updated_at": time.time()})
        print(json.dumps(report, ensure_ascii=False), flush=True)
        return report
    finally:
        for frame in frames.values():
            frame.unpersist()
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect", action="store_true")
    parser.add_argument("--duration", type=int, default=50)
    args = parser.parse_args()
    try:
        if args.collect:
            collect_experiments(args.duration)
        train_windows()
    except Exception as error:
        atomic_report(PROJECT_ROOT / "outputs/live_lab/preparation.json", {"status": "failed", "error": str(error), "updated_at": time.time()})
        raise


if __name__ == "__main__":
    main()

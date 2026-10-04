"""Score complete Docker traffic runs with Spark Structured Streaming and MLlib."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.lab_features import FLOW_SCHEMA, SESSION_FEATURES, aggregate_sessions
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


def run_stream(*, once: bool = False, trigger_seconds: int = 3) -> None:
    if trigger_seconds < 1:
        raise ValueError("trigger_seconds must be at least 1")
    config = load_config()
    model_root = PROJECT_ROOT / "models" / "live_lab"
    model_path = model_root / "session_pipeline"
    metadata_path = model_root / "metrics.json"
    if not model_path.exists() or not metadata_path.is_file():
        raise FileNotFoundError("Run the training collection and `python -m src.lab_model` first.")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    threshold = float(metadata["lab_sessions"]["threshold"])
    incoming = PROJECT_ROOT / "data" / "lab" / "incoming" / "demo"
    output = PROJECT_ROOT / "outputs" / "live_lab" / "sessions"
    predictions_dir = output / "predictions"
    summaries_dir = output / "summaries"
    checkpoint = output / "checkpoint_demo"
    incoming.mkdir(parents=True, exist_ok=True)
    predictions_dir.mkdir(parents=True, exist_ok=True)
    summaries_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session(config, app_name="spark-ids-docker-live-sessions")
    try:
        model = PipelineModel.load(str(model_path))
        source = (
            spark.readStream.schema(FLOW_SCHEMA)
            .option("maxFilesPerTrigger", 50)
            .json(str(incoming))
            .where(
                (F.col("lab_mode") == "demo")
                & (F.col("record_type") == "flow")
                & F.col("run_id").isNotNull()
            )
        )

        def score_batch(frame, batch_id: int) -> None:
            if not frame.take(1):
                return
            started = time.perf_counter()
            all_demo_flows = (
                spark.read.schema(FLOW_SCHEMA).json(str(incoming))
                .where(
                    (F.col("lab_mode") == "demo")
                    & (F.col("record_type") == "flow")
                    & F.col("run_id").isNotNull()
                )
            )
            scored = (
                model.transform(aggregate_sessions(all_demo_flows))
                .withColumn("attack_probability", vector_to_array("probability")[1])
                .withColumn("prediction", (F.col("attack_probability") >= F.lit(threshold)).cast("int"))
                .cache()
            )
            summary = scored.agg(
                F.count("*").alias("runs"),
                F.sum("prediction").alias("alerts"),
                F.sum("flow_count").alias("flows"),
                F.avg("attack_probability").alias("mean_attack_probability"),
            ).first()
            (
                scored.select(
                    "run_id", *SESSION_FEATURES, "attack_probability", "prediction"
                )
                .write.mode("overwrite")
                .json(str(predictions_dir))
            )
            report = {
                "batch_id": int(batch_id),
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "runs_scored": int(summary["runs"] or 0),
                "flows_scored": int(summary["flows"] or 0),
                "alerts": int(summary["alerts"] or 0),
                "mean_attack_probability": float(summary["mean_attack_probability"] or 0.0),
                "threshold": threshold,
                "processing_seconds": time.perf_counter() - started,
            }
            temporary = summaries_dir / ".latest.tmp"
            temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
            temporary.replace(summaries_dir / "latest.json")
            scored.unpersist()
            print(json.dumps(report), flush=True)

        query = source.writeStream.foreachBatch(score_batch).option(
            "checkpointLocation", str(checkpoint)
        )
        query = query.trigger(availableNow=True) if once else query.trigger(
            processingTime=f"{trigger_seconds} seconds"
        )
        query.start().awaitTermination()
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Process current files and stop.")
    parser.add_argument("--trigger-seconds", type=int, default=3)
    args = parser.parse_args()
    run_stream(once=args.once, trigger_seconds=args.trigger_seconds)


if __name__ == "__main__":
    main()

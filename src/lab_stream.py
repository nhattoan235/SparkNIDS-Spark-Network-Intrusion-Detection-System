"""Incrementally score observed-IP windows with Spark state and MLlib."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.lab_features import FLOW_SCHEMA, WATERMARK_SECONDS, WINDOW_FEATURES, WINDOW_SECONDS, aggregate_windows
from src.lab_window_model import atomic_report
from src.spark_session import PROJECT_ROOT, create_spark_session, load_config


def _batch_measurement(progress: dict) -> dict:
    input_rows = int(progress.get("numInputRows", 0) or 0)
    duration_seconds = float(progress.get("durationMs", {}).get("triggerExecution", 0) or 0) / 1000
    return {
        "batch_id": int(progress.get("batchId", 0)),
        "timestamp": progress.get("timestamp"),
        "input_rows": input_rows,
        "duration_seconds": duration_seconds,
        "flow_per_second": input_rows / duration_seconds if input_rows and duration_seconds > 0 else None,
    }


def window_key(record: dict) -> str:
    return f"{record['experiment_id']}|{record['source_ip']}|{record['window_start']}"


def to_utc_iso(value: datetime, local_timezone=None) -> str:
    """PySpark returns naive timestamps in the host's local zone; store UTC explicitly."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=local_timezone or datetime.now().astimezone().tzinfo)
    return value.astimezone(timezone.utc).isoformat()


def run_stream(*, once: bool = False, trigger_seconds: int = 3) -> None:
    if trigger_seconds < 1:
        raise ValueError("trigger_seconds must be at least 1")
    model_root = PROJECT_ROOT / "models/live_lab"
    metadata = json.loads((model_root / "window_metrics.json").read_text(encoding="utf-8"))
    if metadata.get("window_seconds") != WINDOW_SECONDS:
        raise ValueError("Model and stream window durations differ; retrain the window model")
    threshold = float(metadata["threshold"])
    incoming = PROJECT_ROOT / "data/lab/incoming/live"
    output = PROJECT_ROOT / "outputs/live_lab/windows"
    incoming.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)
    snapshot_path = output / "windows.json"
    try:
        history = {window_key(row): row for row in json.loads(snapshot_path.read_text(encoding="utf-8"))["rows"]}
    except (OSError, ValueError, KeyError):
        history = {}
    config = load_config()
    config["spark"]["settings"]["spark.sql.session.timeZone"] = "UTC"
    spark = create_spark_session(config, app_name="spark-ids-live-ip-windows")
    query = None
    progress_path = output / "progress.json"
    batch_report = {}
    recent_batches = []
    atomic_report(progress_path, {"status": "starting", "heartbeat": time.time()})
    try:
        model = PipelineModel.load(str(model_root / "window_pipeline"))
        source = spark.readStream.schema(FLOW_SCHEMA).option("maxFilesPerTrigger", 50).json(str(incoming)).where(
            (F.col("record_type") == "flow") & (F.col("lab_mode") == "live")
        )
        windows = aggregate_windows(source)

        def score_batch(frame, batch_id: int):
            started = time.perf_counter()
            scored = model.transform(frame).withColumn("attack_probability", vector_to_array("probability")[1]).withColumn(
                "prediction", (F.col("attack_probability") >= threshold).cast("int")
            )
            # Fully consume each updated state batch; only per-source windows reach Python.
            rows = scored.select("experiment_id", "source_ip", "actor_id", "window_start", "window_end",
                *WINDOW_FEATURES, "attack_probability", "prediction").collect()
            for row in rows:
                record = row.asDict()
                for column in ("window_start", "window_end"):
                    record[column] = to_utc_iso(record[column])
                record.update(scored_at=time.time(), batch_id=int(batch_id), threshold=threshold)
                history[window_key(record)] = record
            # ponytail: retain 5000 viewer windows; use a database sink for long-running deployment.
            if len(history) > 5000:
                keep = sorted(history.values(), key=lambda item: item["window_start"])[-5000:]
                history.clear()
                history.update((window_key(row), row) for row in keep)
            if rows:
                atomic_report(snapshot_path, {"rows": list(history.values()), "updated_at": time.time(),
                    "window_seconds": WINDOW_SECONDS, "watermark_seconds": WATERMARK_SECONDS})
            batch_report.update(batch_id=int(batch_id), updated_windows=len(rows),
                scoring_seconds=time.perf_counter() - started)

        writer = windows.writeStream.outputMode("update").foreachBatch(score_batch).option(
            "checkpointLocation", str(output / "checkpoint_v1")
        )
        writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=f"{trigger_seconds} seconds")
        query = writer.start()
        previous_batch = None
        while query.isActive:
            latest = query.lastProgress or {}
            new_batch = bool(latest and latest.get("batchId") != previous_batch)
            if new_batch:
                recent_batches.append(_batch_measurement(latest))
                del recent_batches[:-30]
                previous_batch = latest.get("batchId")
            report = {"status": "running", "heartbeat": time.time(), "master": spark.sparkContext.master,
                "spark_ui_url": spark.sparkContext.uiWebUrl, "window_seconds": WINDOW_SECONDS,
                "watermark_seconds": WATERMARK_SECONDS, "threshold": threshold,
                "stream_batch_id": latest.get("batchId"), **batch_report,
                "recent_batches": recent_batches,
                "input_rows": latest.get("numInputRows", 0),
                "input_rows_per_second": latest.get("inputRowsPerSecond", 0),
                "processed_rows_per_second": latest.get("processedRowsPerSecond", 0),
                "batch_seconds": latest.get("durationMs", {}).get("triggerExecution", 0) / 1000,
                "event_time": latest.get("eventTime", {}), "state": latest.get("stateOperators", [])}
            atomic_report(progress_path, report)
            if new_batch:
                print(json.dumps({key: report[key] for key in ("batch_id", "input_rows", "batch_seconds") if key in report}), flush=True)
            query.awaitTermination(2)
        query.awaitTermination()
        atomic_report(progress_path, {"status": "stopped", "heartbeat": time.time(), **batch_report})
    except Exception as error:
        atomic_report(progress_path, {"status": "failed", "heartbeat": time.time(), "error": str(error)})
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--trigger-seconds", type=int, default=3)
    args = parser.parse_args()
    run_stream(once=args.once, trigger_seconds=args.trigger_seconds)


if __name__ == "__main__":
    main()

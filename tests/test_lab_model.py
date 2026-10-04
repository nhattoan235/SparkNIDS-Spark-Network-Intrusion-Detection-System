import json

import pytest
from pyspark.ml.linalg import Vectors

import src.lab_model as lab_model
from src.lab_model import (
    LAB_FEATURES,
    SESSION_FEATURES,
    build_lab_pipeline,
    build_session_pipeline,
    select_session_training_mode,
    stratified_run_split,
)
from src.spark_session import create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session(app_name="lab-model-test")
    yield session
    session.stop()


def test_lab_model_scores_flow_without_unsw_only_columns(spark):
    train = spark.createDataFrame(
        [
            (1, 0.1, "tcp", "http", 100, 200, 0),
            (2, 0.2, "tcp", "http", 110, 210, 0),
            (3, 0.01, "tcp", "-", 0, 0, 1),
            (4, 0.02, "tcp", "-", 0, 0, 1),
        ],
        "id int, dur double, proto string, service string, sbytes long, dbytes long, label int",
    )
    live = spark.createDataFrame(
        [("abc", 0.02, "tcp", "-", 0, 0)],
        "event_id string, dur double, proto string, service string, sbytes long, dbytes long",
    )
    model = build_lab_pipeline(num_trees=2, max_depth=2).fit(train)
    result = model.transform(live).select("event_id", "probability").first()
    assert result.event_id == "abc"
    assert 0 <= result.probability[1] <= 1
    assert LAB_FEATURES == ("dur", "sbytes", "dbytes", "proto", "service")


def test_session_model_scores_aggregated_run_features(spark):
    schema = (
        "run_id string, flow_count double, destination_port_count double, elapsed_seconds double, "
        "flow_rate double, mean_sbytes double, mean_dbytes double, mean_duration double, "
        "http_ratio double, label int"
    )
    rows = [
        ("normal-a", 12.0, 1.0, 3.0, 4.0, 98.0, 60.0, 0.01, 1.0, 0),
        ("normal-b", 16.0, 1.0, 4.0, 4.0, 98.0, 60.0, 0.01, 1.0, 0),
        ("scan-a", 10.0, 10.0, 0.4, 22.5, 20.0, 3.0, 0.01, 0.0, 1),
        ("scan-b", 12.0, 12.0, 0.5, 24.0, 20.0, 3.0, 0.01, 0.0, 1),
    ]
    training = spark.createDataFrame(rows, schema)
    live = spark.createDataFrame(
        [("new-run", 11.0, 11.0, 0.5, 20.0, 20.0, 3.0, 0.01, 0.0)],
        schema.rsplit(", label int", 1)[0],
    )

    result = (
        build_session_pipeline(num_trees=3, max_depth=2)
        .fit(training)
        .transform(live)
        .select("run_id", "probability")
        .first()
    )

    assert result.run_id == "new-run"
    assert 0 <= result.probability[1] <= 1
    assert "flow_rate" in SESSION_FEATURES


def test_run_split_keeps_complete_sessions_and_each_label_in_all_splits():
    run_rows = [(f"normal-{index}", 0) for index in range(10)]
    run_rows.extend((f"attack-{index}", 1) for index in range(10))

    splits = stratified_run_split(run_rows)

    assert set(splits) == {"train", "validation", "test"}
    assert not set(splits["train"]) & set(splits["validation"])
    assert not set(splits["train"]) & set(splits["test"])
    assert not set(splits["validation"]) & set(splits["test"])
    for name in splits:
        assert any(run.startswith("normal-") for run in splits[name])
        assert any(run.startswith("attack-") for run in splits[name])


def test_session_threshold_chooses_lower_value_when_validation_quality_ties():
    curve = [
        {"threshold": 0.2, "f1": 0.973, "recall": 1.0, "false_positive_rate": 0.111},
        {"threshold": 0.3, "f1": 1.0, "recall": 1.0, "false_positive_rate": 0.0},
        {"threshold": 0.5, "f1": 1.0, "recall": 1.0, "false_positive_rate": 0.0},
        {"threshold": 0.6, "f1": 1.0, "recall": 1.0, "false_positive_rate": 0.0},
    ]

    selected = lab_model.select_session_threshold(curve)

    assert selected["threshold"] == 0.3


def test_session_model_prefers_complete_calibration_runs():
    rows = [(f"cal-normal-{index}", 0, "calibration") for index in range(5)]
    rows.extend((f"cal-attack-{index}", 1, "calibration") for index in range(5))
    rows.extend((f"old-{index}", index % 2, "training") for index in range(20))

    assert select_session_training_mode(rows) == "calibration"


def test_session_model_falls_back_when_calibration_is_too_small():
    rows = [(f"cal-normal-{index}", 0, "calibration") for index in range(4)]
    rows.extend((f"cal-attack-{index}", 1, "calibration") for index in range(5))
    rows.extend((f"old-normal-{index}", 0, "training") for index in range(5))
    rows.extend((f"old-attack-{index}", 1, "training") for index in range(5))

    assert select_session_training_mode(rows) == "training"


def test_complete_lab_sessions_exclude_incomplete_runs(spark, tmp_path):
    incoming = tmp_path / "incoming" / "training"
    incoming.mkdir(parents=True)
    run_truth = tmp_path / "truth" / "runs"
    run_truth.mkdir(parents=True)
    for run_id, flow_count in (("complete", 2), ("partial", 1)):
        (run_truth / f"{run_id}.json").write_text(
            json.dumps({
                "run_id": run_id,
                "scenario": "normal",
                "label": 0,
                "lab_mode": "training",
                "expected_flows": 2,
            }),
            encoding="utf-8",
        )
        for index in range(flow_count):
            (incoming / f"{run_id}-{index}.json").write_text(
                json.dumps({
                    "run_id": run_id,
                    "lab_mode": "training",
                    "record_type": "flow",
                    "timestamp": f"2026-10-02T10:00:0{index}+00:00",
                    "destination_port": 8080,
                    "service": "http",
                    "sbytes": 100,
                    "dbytes": 40,
                    "dur": 0.01,
                }),
                encoding="utf-8",
            )

    sessions = lab_model.load_complete_lab_sessions(
        spark, incoming=tmp_path / "incoming", run_truth=run_truth
    )

    assert [row.run_id for row in sessions.select("run_id").collect()] == ["complete"]


def test_challenge_evaluation_uses_only_unseen_training_runs(spark):
    predictions = spark.createDataFrame(
        [
            ("familiar-normal", 0, "calibration", Vectors.dense(0.1, 0.9)),
            ("familiar-attack", 1, "calibration", Vectors.dense(0.9, 0.1)),
            ("busy-normal", 0, "training", Vectors.dense(0.2, 0.8)),
            ("slow-scan", 1, "training", Vectors.dense(0.1, 0.9)),
        ],
        ["run_id", "label", "lab_mode", "probability"],
    )

    result = lab_model.evaluate_challenge_runs(predictions, threshold=0.3)

    assert result["source_mode"] == "training"
    assert result["total"] == 2
    assert result["true_positive"] == 1
    assert result["false_positive"] == 1

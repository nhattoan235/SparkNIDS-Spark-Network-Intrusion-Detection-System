import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from dashboard.live_lab_controller import LiveLabController
from dashboard.live_view import TruthIndex, epoch
from lab import generator
from src import lab_features
from src.lab_stream import to_utc_iso
from src.spark_session import create_spark_session


def test_live_profile_commands_are_atomic_and_do_not_change_ground_truth(tmp_path):
    controller = LiveLabController(tmp_path)
    assert hasattr(controller, "set_behavior"), "Live behavior controls are missing"
    controller._write_manifest({"experiment_id": "a" * 12, "kind": "live", "status": "running", "senders": [{"actor_id": "sender-01"}]})
    controller.set_behavior("a" * 12, "sender-01", "busy", 4.0)
    record = json.loads((tmp_path / "data/lab/control" / ("a" * 12 + ".sender-01.json")).read_text())
    assert record["profile"] == "busy"
    assert record["rate"] == 4.0
    assert "label" not in record
    with pytest.raises(ValueError):
        controller.set_behavior("a" * 12, "sender-99", "scan", 2.0)
    with pytest.raises(ValueError):
        controller.set_behavior("a" * 12, "sender-01", "normal", 10000.0)


def test_live_schedule_starts_normal_and_returns_to_normal():
    assert hasattr(generator, "live_settings"), "Live traffic schedule is missing"
    assert generator.live_settings(0, "sender-01", seed=42, automatic=True)["profile"] == "normal"
    assert generator.live_settings(25, "sender-01", seed=42, automatic=True)["profile"] in {"normal", "busy", "scan", "slow_scan", "burst", "pulsed"}
    assert generator.live_settings(65, "sender-01", seed=42, automatic=True)["profile"] == "normal"


def test_spark_local_timestamp_is_converted_to_utc_before_saving():
    local_time = datetime(2026, 10, 9, 15, 30, 20)
    assert to_utc_iso(local_time, ZoneInfo("Asia/Bangkok")) == "2026-10-09T08:30:20+00:00"
    assert epoch("2026-10-09T15:30:20+07:00") == epoch("2026-10-09T08:30:20Z")


def test_completed_run_can_check_labels_without_a_later_event_time_watermark(tmp_path):
    truth_path = tmp_path / "truth.jsonl"
    truth_path.write_text(json.dumps({
        "timestamp": "2026-10-09T08:30:21+00:00",
        "source_ip": "172.20.0.3",
        "label": 0,
        "scenario": "normal",
    }) + "\n", encoding="utf-8")
    truth = TruthIndex()
    truth.update(tmp_path)
    row = {
        "source_ip": "172.20.0.3",
        "window_start": "2026-10-09T08:30:20+00:00",
        "window_end": "2026-10-09T08:30:30+00:00",
        "flow_count": 1,
        "prediction": 0,
    }

    result = truth.check(row, watermark=None, run_complete=True)

    assert result == {"checked": True, "outcome": "Bình thường đúng", "actual": 0}


def test_spark_groups_observed_source_in_ten_second_windows_without_labels():
    assert hasattr(lab_features, "aggregate_windows"), "Time-window aggregation is missing"
    spark = create_spark_session(app_name="live-window-behavior-test")
    try:
        flows = spark.createDataFrame([
            ("e1", "exp1", "s1", "10.0.0.1", "2026-10-09T10:00:01Z", 8080, "http", 100, 60, 0.01),
            ("e2", "exp1", "s1", "10.0.0.1", "2026-10-09T10:00:02Z", 9001, "-", 50, 3, 0.02),
            ("e3", "exp1", "s1", "10.0.0.1", "2026-10-09T10:00:11Z", 8080, "http", 100, 60, 0.01),
            ("e4", "exp1", "s2", "10.0.0.2", "2026-10-09T10:00:02Z", 8080, "http", 100, 60, 0.01),
            ("e5", "exp2", "s1", "10.0.0.1", "2026-10-09T10:00:02Z", 8080, "http", 100, 60, 0.01),
        ], "event_id string, experiment_id string, actor_id string, source_ip string, timestamp string, destination_port int, service string, sbytes long, dbytes long, dur double")
        rows = lab_features.aggregate_windows(flows).collect()
        assert len(rows) == 4
        combined = next(row for row in rows if row.flow_count == 2)
        assert combined.port_scan_ratio == pytest.approx(0.5)
        assert combined.flow_rate == pytest.approx(0.2)
        assert combined.http_ratio == pytest.approx(0.5)
        assert "label" not in combined.asDict()
    finally:
        spark.stop()

import json
import socket
import threading
import uuid
from collections import Counter

import pytest

from dashboard import live_lab_controller
from dashboard import live_view
from lab import generator as lab_generator
from lab.generator import scenario_label, write_run_truth, write_truth
from lab.victim import make_server


@pytest.mark.parametrize("lab_mode", ["demo", "training", "calibration"])
def test_http_connection_creates_flow_without_scenario_label(tmp_path, lab_mode):
    server = make_server(0, "http", tmp_path / "incoming")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    event_id = uuid.uuid4().hex
    run_id = uuid.uuid4().hex
    try:
        with socket.create_connection(("127.0.0.1", server.server_address[1]), timeout=2) as client:
            request = (
                f"GET / HTTP/1.1\r\nHost: victim\r\nX-Lab-Event: {event_id}\r\n"
                f"X-Lab-Run: {run_id}\r\nX-Lab-Mode: {lab_mode}\r\n\r\n"
            ).encode()
            client.sendall(request)
            assert b"200 OK" in client.recv(1024)
        flow = json.loads((tmp_path / "incoming" / lab_mode / f"{event_id}.json").read_text())
        assert flow["event_id"] == event_id
        assert flow["run_id"] == run_id
        assert flow["lab_mode"] == lab_mode
        assert flow["service"] == "http"
        assert flow["sbytes"] == len(request)
        assert flow["dbytes"] > 0
        assert flow["dur"] >= 0
        assert "label" not in flow
        assert "scenario" not in flow
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_scenario_truth_is_stored_separately(tmp_path):
    event_id = uuid.uuid4().hex
    assert scenario_label("normal") == 0
    assert scenario_label("scan") == 1
    assert scenario_label("burst") == 1
    write_truth(tmp_path, event_id, "scan")
    assert json.loads((tmp_path / f"{event_id}.json").read_text()) == {
        "event_id": event_id,
        "scenario": "scan",
        "label": 1,
    }


def test_run_truth_records_expected_flow_count_and_mode(tmp_path):
    run_id = uuid.uuid4().hex
    write_run_truth(tmp_path, run_id, "burst", "training", expected_flows=24)
    assert json.loads((tmp_path / "runs" / f"{run_id}.json").read_text()) == {
        "run_id": run_id,
        "scenario": "burst",
        "label": 1,
        "lab_mode": "training",
        "expected_flows": 24,
    }


def test_training_generator_runs_balanced_batches_concurrently(tmp_path, monkeypatch):
    barrier = threading.Barrier(15, timeout=2)
    scenarios = []
    lock = threading.Lock()

    def record_run(scenario, _truth_dir, **kwargs):
        with lock:
            scenarios.append(scenario)
        barrier.wait()
        return 1

    monkeypatch.setattr(lab_generator, "generate", record_run)

    total_flows = lab_generator.generate_training(tmp_path, runs_per_scenario=5)

    assert total_flows == 15
    assert Counter(scenarios) == {"normal": 5, "scan": 5, "burst": 5}


def test_burst_generator_sends_connections_concurrently(tmp_path, monkeypatch):
    barrier = threading.Barrier(8, timeout=2)
    sent = []

    def record_send(*_args):
        barrier.wait()
        sent.append(True)

    monkeypatch.setattr(lab_generator, "send_event", record_send)

    total = lab_generator.generate("burst", tmp_path, flow_count=8, interval_seconds=0)

    assert total == 8
    assert len(sent) == 8


def test_calibration_generator_matches_live_scenarios(tmp_path, monkeypatch):
    barrier = threading.Barrier(15, timeout=2)
    observed = []

    def record_run(scenario, _truth_dir, **kwargs):
        observed.append((scenario, kwargs))
        barrier.wait()
        return kwargs["flow_count"]

    monkeypatch.setattr(lab_generator, "generate", record_run)

    total = lab_generator.generate_training(
        tmp_path, runs_per_scenario=5, lab_mode="calibration"
    )

    assert total == 270
    assert Counter(scenario for scenario, _ in observed) == {
        "normal": 5,
        "scan": 5,
        "burst": 5,
    }
    for scenario, kwargs in observed:
        assert kwargs["lab_mode"] == "calibration"
        expected = {
            "normal": (12, 0.3),
            "scan": (12, 0.05),
            "burst": (30, 0.0),
        }[scenario]
        assert (kwargs["flow_count"], kwargs["interval_seconds"]) == expected
        if scenario == "scan":
            assert kwargs["scan_port_count"] == 12


def test_finds_user_installed_docker_desktop_cli_when_missing_from_path(tmp_path, monkeypatch):
    local_app_data = tmp_path / "LocalAppData"
    docker_exe = local_app_data / "Programs" / "DockerDesktop" / "resources" / "bin" / "docker.exe"
    docker_exe.parent.mkdir(parents=True)
    docker_exe.write_text("placeholder")
    monkeypatch.setattr(live_lab_controller.shutil, "which", lambda _: None)
    monkeypatch.setattr(live_lab_controller.os, "name", "nt")
    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.setenv("ProgramFiles", str(tmp_path / "ProgramFiles"))

    assert live_lab_controller.resolve_docker_cli() == str(docker_exe)


@pytest.mark.parametrize(
    ("manifest", "process_active", "expected"),
    [
        ({"status": "running"}, True, True),
        ({"status": "preparing"}, True, True),
        ({"status": "completed"}, False, False),
        ({"status": "completed"}, True, False),
        ({"status": "running"}, False, False),
    ],
)
def test_stop_control_only_appears_for_an_active_experiment(manifest, process_active, expected):
    assert live_lab_controller.should_offer_stop(manifest, process_active) is expected


def test_latest_live_experiment_can_be_recovered_after_controller_reload(tmp_path):
    controller = live_lab_controller.LiveLabController(tmp_path)
    controller._write_manifest({
        "experiment_id": "a" * 12,
        "kind": "live",
        "lab_mode": "live",
        "created_at": "2026-10-09T10:00:00+00:00",
        "status": "completed",
        "senders": [],
    })
    controller._write_manifest({
        "experiment_id": "b" * 12,
        "kind": "live",
        "lab_mode": "window_test",
        "created_at": "2026-10-09T11:00:00+00:00",
        "status": "completed",
        "senders": [],
    })

    reloaded = live_lab_controller.LiveLabController(tmp_path)

    assert reloaded.latest_live_experiment_id() == "a" * 12


def test_scale_chart_uses_grouped_bars_with_processing_throughput():
    calls = []

    class ChartSink:
        def bar_chart(self, frame, **options):
            calls.append((frame, options))

    frame = object()
    live_view.render_scale_chart(ChartSink(), frame)

    assert calls == [(frame, {
        "x": "Quy mô dữ liệu",
        "y": "Tốc độ Spark (flow/giây)",
        "color": "Partition",
        "x_label": "Số flow Spark đọc",
        "y_label": "Flow được xử lý trong một giây",
    })]


def test_spark_throughput_chart_uses_only_batches_with_measured_input():
    rows = live_view.spark_throughput_rows([
        {"batch_id": 10, "input_rows": 120, "duration_seconds": 0.4, "flow_per_second": 300},
        {"batch_id": 11, "input_rows": 0, "duration_seconds": 0.1, "flow_per_second": None},
    ])

    assert rows == [{"Lô Spark": "#10", "Flow/giây": 300, "Flow trong lô": 120, "Thời gian xử lý (giây)": 0.4}]


def test_spark_throughput_can_measure_the_latest_stream_status_snapshot():
    rows = live_view.spark_throughput_rows([
        {"batch_id": 12, "input_rows": 200, "batch_seconds": 0.5}
    ])

    assert rows == [{"Lô Spark": "#12", "Flow/giây": 400, "Flow trong lô": 200, "Thời gian xử lý (giây)": 0.5}]


def test_remember_spark_batch_keeps_recent_nonempty_batches_once():
    history = []
    batch = {"batch_id": 12, "input_rows": 200, "batch_seconds": 0.5}

    live_view.remember_spark_batch(history, batch)
    live_view.remember_spark_batch(history, batch)
    live_view.remember_spark_batch(history, {"batch_id": 13, "input_rows": 0, "batch_seconds": 0.1})

    assert history == [batch]


def test_window_count_is_explained_as_machine_time_windows_not_connections():
    explanation = live_view.window_count_explanation(135, 15, 9)

    assert explanation == "135 lần chấm = 15 máy × 9 cửa sổ/máy; mỗi cửa sổ gom kết nối trong 10 giây, không phải một kết nối."


def test_window_count_does_not_claim_zero_windows_per_machine():
    assert live_view.window_count_explanation(0, 15, 0) == "Chưa có cửa sổ dữ liệu 10 giây để Spark chấm."


def test_completed_run_reports_spark_done_after_every_window_is_checked():
    assert live_view.stream_feedback("completed", False, 135, 135) == (
        "đã chốt",
        "Spark đã đối chiếu đủ 135 cửa sổ dữ liệu (mỗi cửa sổ là 1 máy trong 10 giây).",
    )

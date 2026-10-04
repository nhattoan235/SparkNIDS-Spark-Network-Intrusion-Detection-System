import json
import socket
import threading
import uuid
from collections import Counter

import pytest

from dashboard import live_lab_controller
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

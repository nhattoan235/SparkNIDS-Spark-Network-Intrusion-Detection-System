"""Generate bounded traffic solely against the Compose lab victim."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import random
import re
import socket
import time
import uuid
from pathlib import Path


SCENARIOS = {"normal": 0, "scan": 1, "burst": 1}
TRAINING_RUNS_PER_SCENARIO = 30
LAB_MODES = {"demo", "training", "calibration"}
SENDER_IDS = tuple(f"sender-{index:02d}" for index in range(1, 16))


def scenario_label(scenario: str) -> int:
    return SCENARIOS[scenario]


def write_truth(
    directory: Path,
    event_id: str,
    scenario: str,
    *,
    run_id: str | None = None,
    lab_mode: str | None = None,
    actor_id: str | None = None,
    experiment_id: str | None = None,
) -> None:
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    record = {"event_id": event_id, "scenario": scenario, "label": scenario_label(scenario)}
    if run_id is not None:
        record["run_id"] = run_id
    if lab_mode is not None:
        record["lab_mode"] = lab_mode
    if actor_id is not None:
        record["actor_id"] = actor_id
    if experiment_id is not None:
        record["experiment_id"] = experiment_id
    temporary = directory / f".{event_id}.tmp"
    temporary.write_text(json.dumps(record), encoding="utf-8")
    os.replace(temporary, directory / f"{event_id}.json")


def write_run_truth(
    directory: Path,
    run_id: str,
    scenario: str,
    lab_mode: str,
    *,
    expected_flows: int,
    actor_id: str | None = None,
    experiment_id: str | None = None,
) -> None:
    if scenario not in SCENARIOS or lab_mode not in LAB_MODES or expected_flows < 1:
        raise ValueError("Run truth requires a known scenario/mode and at least one flow.")
    run_dir = Path(directory) / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": run_id,
        "scenario": scenario,
        "label": scenario_label(scenario),
        "lab_mode": lab_mode,
        "expected_flows": expected_flows,
    }
    if actor_id is not None:
        record["actor_id"] = actor_id
    if experiment_id is not None:
        record["experiment_id"] = experiment_id
    temporary = run_dir / f".{run_id}.tmp"
    temporary.write_text(json.dumps(record), encoding="utf-8")
    os.replace(temporary, run_dir / f"{run_id}.json")


def send_event(
    port: int,
    event_id: str,
    run_id: str,
    lab_mode: str,
    actor_id: str | None = None,
    experiment_id: str | None = None,
) -> None:
    identity_headers = ""
    if actor_id is not None:
        identity_headers += f"X-Lab-Actor: {actor_id}\r\n"
    if experiment_id is not None:
        identity_headers += f"X-Lab-Experiment: {experiment_id}\r\n"
    with socket.create_connection(("victim", port), timeout=3) as client:
        client.settimeout(3)
        if port == 8080:
            client.sendall(
                f"GET / HTTP/1.1\r\nHost: victim\r\nX-Lab-Event: {event_id}\r\n"
                f"X-Lab-Run: {run_id}\r\nX-Lab-Mode: {lab_mode}\r\n"
                f"{identity_headers}Connection: close\r\n\r\n".encode()
            )
        else:
            client.sendall(
                f"EVENT {event_id} RUN_ID: {run_id} LAB_MODE: {lab_mode} "
                f"ACTOR_ID: {actor_id or ''} EXPERIMENT_ID: {experiment_id or ''}\n".encode()
            )
        client.recv(1024)


def generate(
    scenario: str,
    truth_dir: Path,
    *,
    lab_mode: str = "demo",
    run_id: str | None = None,
    flow_count: int | None = None,
    interval_seconds: float | None = None,
    scan_port_count: int | None = None,
    actor_id: str | None = None,
    experiment_id: str | None = None,
) -> int:
    if scenario not in SCENARIOS or lab_mode not in LAB_MODES:
        raise ValueError(f"Unknown scenario: {scenario}")
    run_id = run_id or uuid.uuid4().hex
    if flow_count is None:
        flow_count = 12 if scenario != "burst" else 30
    if interval_seconds is None:
        interval_seconds = {"normal": 0.3, "scan": 0.05, "burst": 0.0}[scenario]
    if scenario == "scan":
        port_count = scan_port_count or 12
        ports = list(range(9001, 9001 + port_count))
    else:
        ports = [8080] * flow_count
    write_run_truth(
        truth_dir,
        run_id,
        scenario,
        lab_mode,
        expected_flows=len(ports),
        actor_id=actor_id,
        experiment_id=experiment_id,
    )
    def send_one(port: int) -> int:
        event_id = uuid.uuid4().hex
        send_event(port, event_id, run_id, lab_mode, actor_id, experiment_id)
        write_truth(
            truth_dir,
            event_id,
            scenario,
            run_id=run_id,
            lab_mode=lab_mode,
            actor_id=actor_id,
            experiment_id=experiment_id,
        )
        return 1

    if scenario == "burst":
        with ThreadPoolExecutor(max_workers=min(len(ports), 10)) as workers:
            return sum(workers.map(send_one, ports))

    sent = 0
    for port in ports:
        sent += send_one(port)
        if sent < len(ports) and interval_seconds > 0:
            time.sleep(interval_seconds)
    return sent


def generate_training(
    truth_dir: Path,
    *,
    runs_per_scenario: int = TRAINING_RUNS_PER_SCENARIO,
    lab_mode: str = "training",
) -> int:
    """Capture varied, labeled runs in balanced concurrent batches."""
    if runs_per_scenario < 5:
        raise ValueError("At least five runs per scenario are needed for train/validation/test splits.")
    if lab_mode not in {"training", "calibration"}:
        raise ValueError("Training collection mode must be training or calibration.")
    rng = random.Random(42)
    total_flows = 0

    def run_training(task: tuple[str, int, float, int | None]) -> int:
        scenario, count, interval, ports = task
        return generate(
            scenario,
            truth_dir,
            lab_mode=lab_mode,
            flow_count=count,
            interval_seconds=interval,
            scan_port_count=ports,
        )

    with ThreadPoolExecutor(max_workers=15) as workers:
        for start in range(0, runs_per_scenario, 5):
            batch_size = min(5, runs_per_scenario - start)
            batch = []
            for scenario in SCENARIOS:
                for _ in range(batch_size):
                    if lab_mode == "calibration" and scenario == "normal":
                        count, interval, ports = 12, 0.3, None
                    elif lab_mode == "calibration" and scenario == "scan":
                        count, interval, ports = 12, 0.05, 12
                    elif lab_mode == "calibration":
                        count, interval, ports = 30, 0.0, None
                    elif scenario == "normal":
                        count = rng.randint(10, 22)
                        interval = rng.uniform(0.10, 0.25)
                        ports = None
                    elif scenario == "scan":
                        ports = rng.randint(6, 12)
                        count = ports
                        interval = rng.uniform(0.025, 0.12)
                    else:
                        count = rng.randint(18, 40)
                        interval = rng.uniform(0.005, 0.04)
                        ports = None
                    batch.append((scenario, count, interval, ports))
            rng.shuffle(batch)
            total_flows += sum(workers.map(run_training, batch))
    return total_flows


def wait_for_start(control_dir: Path, experiment_id: str, actor_id: str, timeout: float = 90) -> None:
    if actor_id not in SENDER_IDS or not re.fullmatch(r"[a-f0-9]{12}", experiment_id):
        raise ValueError("Invalid sender or experiment ID.")
    control_dir.mkdir(parents=True, exist_ok=True)
    ready = control_dir / f"{experiment_id}.{actor_id}.ready"
    gate = control_dir / f"{experiment_id}.go"
    ready.write_text("ready", encoding="utf-8")
    deadline = time.monotonic() + timeout
    try:
        while not gate.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("Timed out waiting for all senders to be ready.")
            time.sleep(0.1)
    finally:
        ready.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=[*SCENARIOS, "training"])
    parser.add_argument("--lab-mode", choices=("training", "calibration"), default="training")
    parser.add_argument("--runs-per-scenario", type=int, default=TRAINING_RUNS_PER_SCENARIO)
    parser.add_argument("--actor-id", choices=SENDER_IDS)
    parser.add_argument("--experiment-id")
    parser.add_argument("--run-id")
    parser.add_argument("--wait-for-start", action="store_true")
    args = parser.parse_args()
    truth_dir = Path("/lab/truth")
    if args.scenario == "training":
        count = generate_training(
            truth_dir,
            runs_per_scenario=args.runs_per_scenario,
            lab_mode=args.lab_mode,
        )
        print(
            f"{args.lab_mode}: {args.runs_per_scenario * len(SCENARIOS)} runs, {count} connections",
            flush=True,
        )
    else:
        if args.wait_for_start:
            if not args.actor_id or not args.experiment_id:
                parser.error("--wait-for-start requires --actor-id and --experiment-id")
            wait_for_start(Path("/lab/control"), args.experiment_id, args.actor_id)
        count = generate(
            args.scenario,
            truth_dir,
            actor_id=args.actor_id,
            experiment_id=args.experiment_id,
            run_id=args.run_id,
        )
        print(f"{args.scenario}: {count} connections", flush=True)


if __name__ == "__main__":
    main()

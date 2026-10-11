"""Generate bounded traffic solely against the Compose lab victim."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import random
import re
import socket
import threading
import time
import uuid
from pathlib import Path


SCENARIOS = {"normal": 0, "scan": 1, "burst": 1}
TRAINING_RUNS_PER_SCENARIO = 30
LAB_MODES = {"demo", "training", "calibration"}
SENDER_IDS = tuple(f"sender-{index:02d}" for index in range(1, 16))
LIVE_LABELS = {"normal": 0, "busy": 0, "scan": 1, "slow_scan": 1, "burst": 1, "pulsed": 1}
LIVE_NAMES = {"normal": "Truy cập thường", "busy": "Truy cập nhanh hợp lệ", "scan": "Quét nhiều cổng", "slow_scan": "Quét cổng chậm", "burst": "Gửi dồn", "pulsed": "Gửi dồn ngắt quãng"}
LIVE_RATES = {"normal": 0.7, "busy": 4.0, "scan": 3.0, "slow_scan": 0.7, "burst": 14.0, "pulsed": 14.0}
LIVE_MODES = {"live", "window_train", "window_validation", "window_test"}
TRUTH_APPEND_LOCK = threading.Lock()


def scenario_label(scenario: str) -> int:
    return LIVE_LABELS[scenario]


def write_truth(
    directory: Path,
    event_id: str,
    scenario: str,
    *,
    run_id: str | None = None,
    lab_mode: str | None = None,
    actor_id: str | None = None,
    experiment_id: str | None = None,
    observed: dict | None = None,
) -> None:
    directory = Path(directory)
    if lab_mode in LIVE_MODES and experiment_id:
        directory = directory / "events" / experiment_id
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
    if observed:
        record.update(observed)
    if lab_mode in LIVE_MODES and actor_id:
        with TRUTH_APPEND_LOCK, (directory / f"{actor_id}.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record) + "\n")
            stream.flush()
        return
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
    if scenario not in LIVE_LABELS or lab_mode not in LAB_MODES | LIVE_MODES or expected_flows < 1:
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
) -> dict:
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
        reply = client.recv(1024)
        observed = re.search(rb"(?:X-Lab-Observed-At: |TIME )([^\r\n ]+)", reply)
        return {"source_ip": client.getsockname()[0], "timestamp": observed.group(1).decode()} if observed else {}


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
        try:
            start_at = json.loads(gate.read_text(encoding="utf-8")).get("start_at", 0)
        except (ValueError, OSError):
            start_at = 0
        while time.time() < start_at:
            time.sleep(min(0.1, start_at - time.time()))
    finally:
        ready.unlink(missing_ok=True)


def live_settings(elapsed: float, actor_id: str, *, seed: int, automatic: bool, training: bool = False) -> dict:
    """Reproducible varied traffic; the seed changes behavior, not model answers."""
    actor = int(actor_id.rsplit("-", 1)[1]) - 1
    if training:
        rng = random.Random(seed * 1000 + actor * 97 + int(elapsed // 10))
        profile = tuple(LIVE_LABELS)[(actor + seed) % len(LIVE_LABELS)]
        ranges = {"normal": (0.3, 1.4), "busy": (2.0, 7.0), "scan": (1.0, 5.0), "slow_scan": (0.3, 1.0), "burst": (8.0, 20.0), "pulsed": (9.0, 20.0)}
        return {"profile": profile, "rate": rng.uniform(*ranges[profile])}
    if not automatic or elapsed < 15 or elapsed >= 60:
        return {"profile": "normal", "rate": LIVE_RATES["normal"]}
    actors = list(SENDER_IDS)
    random.Random(seed).shuffle(actors)
    slot = actors.index(actor_id)
    profiles = ["normal"] * 5 + ["busy"] * 3 + ["scan"] * 3 + ["burst"] * 2 + ["slow_scan", "pulsed"]
    profile = profiles[slot]
    return {"profile": profile, "rate": LIVE_RATES[profile]}


def run_live(truth_dir: Path, control_dir: Path, *, actor_id: str, experiment_id: str,
             duration: float, seed: int, automatic: bool, lab_mode: str = "live") -> int:
    if actor_id not in SENDER_IDS or not re.fullmatch(r"[a-f0-9]{12}", experiment_id):
        raise ValueError("Invalid live sender identity")
    if lab_mode not in LIVE_MODES or not 10 <= duration <= 600:
        raise ValueError("Live runs require a known mode and a duration of 10–600 seconds")
    control_path = control_dir / f"{experiment_id}.{actor_id}.json"
    status_path = control_dir / f"{experiment_id}.{actor_id}.status.json"
    run_id = uuid.uuid4().hex
    started = time.monotonic()
    deadline = started + duration
    sent = failed = port_cursor = 0
    with ThreadPoolExecutor(max_workers=4) as workers:
        while time.monotonic() < deadline:
            cycle_started = time.monotonic()
            elapsed = cycle_started - started
            settings = live_settings(elapsed, actor_id, seed=seed, automatic=automatic, training=lab_mode != "live")
            try:
                override = json.loads(control_path.read_text(encoding="utf-8"))
                if override.get("stop"):
                    break
                if override.get("profile") in LIVE_LABELS:
                    settings = override
            except (OSError, ValueError):
                pass
            profile = settings["profile"]
            rate = min(25.0, max(0.2, float(settings["rate"])))
            idle = profile == "pulsed" and int(elapsed) % 8 >= 3
            count = 0 if idle else max(1, round(rate)) if rate >= 1 else 1
            ports = []
            for _ in range(count):
                ports.append(9001 + port_cursor % 12 if profile in {"scan", "slow_scan"} else 8080)
                port_cursor += 1

            def emit(port: int) -> int:
                event_id = uuid.uuid4().hex
                try:
                    observation = send_event(port, event_id, run_id, lab_mode, actor_id, experiment_id)
                    write_truth(truth_dir, event_id, profile, run_id=run_id, lab_mode=lab_mode,
                                actor_id=actor_id, experiment_id=experiment_id, observed=observation)
                    return 1
                except OSError:
                    return 0

            successes = sum(workers.map(emit, ports))
            sent += successes
            failed += len(ports) - successes
            temporary = status_path.with_name(f".{status_path.name}.{uuid.uuid4().hex}.tmp")
            temporary.write_text(json.dumps({"profile": profile, "rate": rate, "sent": sent,
                "failed": failed, "elapsed": elapsed, "updated_at": time.time()}), encoding="utf-8")
            for attempt in range(8):
                try:
                    os.replace(temporary, status_path)
                    break
                except PermissionError:
                    if attempt == 7:
                        raise
                    time.sleep(0.05 * (attempt + 1))
            pause = (1.0 / rate if rate < 1 else 1.0) - (time.monotonic() - cycle_started)
            if pause > 0:
                time.sleep(min(pause, max(0, deadline - time.monotonic())))
    print(json.dumps({"actor_id": actor_id, "sent": sent, "failed": failed}), flush=True)
    return sent


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=[*SCENARIOS, "training", "live"])
    parser.add_argument("--lab-mode", choices=sorted(LAB_MODES | LIVE_MODES), default="training")
    parser.add_argument("--runs-per-scenario", type=int, default=TRAINING_RUNS_PER_SCENARIO)
    parser.add_argument("--actor-id", choices=SENDER_IDS)
    parser.add_argument("--experiment-id")
    parser.add_argument("--run-id")
    parser.add_argument("--wait-for-start", action="store_true")
    parser.add_argument("--duration", type=float, default=90)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--automatic", action="store_true")
    args = parser.parse_args()
    truth_dir = Path("/lab/truth")
    if args.scenario == "live":
        if not args.actor_id or not args.experiment_id:
            parser.error("live requires --actor-id and --experiment-id")
        if args.wait_for_start:
            wait_for_start(Path("/lab/control"), args.experiment_id, args.actor_id)
        run_live(truth_dir, Path("/lab/control"), actor_id=args.actor_id, experiment_id=args.experiment_id,
                 duration=args.duration, seed=args.seed, automatic=args.automatic, lab_mode=args.lab_mode)
    elif args.scenario == "training":
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

"""Start and supervise one isolated, synchronized Docker traffic experiment."""

from __future__ import annotations

import json
import math
import os
import random
import shutil
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from lab.generator import LIVE_LABELS, LIVE_NAMES, LIVE_RATES


SENDER_IDS = tuple(f"sender-{index:02d}" for index in range(1, 16))
SCENARIO_ORDER = ("normal", "scan", "burst")
SCENARIO_FLOWS = {"normal": 12, "scan": 12, "burst": 30}
SCENARIO_NAMES = {"normal": "Bình thường", "scan": "Quét cổng", "burst": "Gửi dồn"}


def resolve_docker_cli() -> str:
    docker = shutil.which("docker")
    if docker:
        return docker
    if os.name == "nt":
        candidates = (
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Docker" / "Docker" / "resources" / "bin" / "docker.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "DockerDesktop" / "resources" / "bin" / "docker.exe",
        )
        if docker := next((str(path) for path in candidates if path.is_file()), None):
            return docker
    raise FileNotFoundError("Không tìm thấy docker.exe. Hãy thêm Docker CLI vào PATH rồi mở lại Streamlit.")


def build_assignments(mode: str, seed: int, counts: dict[str, int] | None = None) -> dict[str, str]:
    if mode == "Tự động":
        scenarios = ["normal"] * 5 + ["scan"] * 5 + ["burst"] * 5
        random.Random(seed).shuffle(scenarios)
        return dict(zip(SENDER_IDS, scenarios, strict=True))
    if mode != "Tự chọn" or counts is None:
        raise ValueError("Hãy chọn Tự động hoặc nhập số lượng từng kịch bản.")
    if set(counts) != set(SCENARIO_ORDER) or any(value < 0 for value in counts.values()):
        raise ValueError("Số lượng kịch bản không hợp lệ.")
    if sum(counts.values()) != len(SENDER_IDS) or counts["normal"] < 1:
        raise ValueError("Cần đúng 15 máy và ít nhất 1 máy bình thường để làm mốc.")
    return dict(zip(SENDER_IDS, [scenario for scenario in SCENARIO_ORDER for _ in range(counts[scenario])], strict=True))


def should_offer_stop(manifest: dict, process_active: bool) -> bool:
    return process_active and manifest.get("status") in {"preparing", "running"}


class LiveLabController:
    def __init__(self, project_root: Path):
        self.root = Path(project_root).resolve()
        self.compose_file = self.root / "lab" / "compose.yaml"
        self.control_dir = self.root / "data" / "lab" / "control"
        self.truth_dir = self.root / "data" / "lab" / "truth"
        self.experiments_dir = self.root / "outputs" / "live_lab" / "experiments"
        self.logs_dir = self.root / "outputs" / "live_lab" / "actors"
        self._processes: dict[str, subprocess.Popen] = {}
        self._container_names: dict[str, str] = {}
        self._experiment_id: str | None = None
        self._stopped = False
        self._stream_process = None
        self._preparation_process = None
        self._scale_process = None

    @property
    def active(self) -> bool:
        return any(process.poll() is None for process in self._processes.values())

    def start_experiment(
        self,
        assignments: dict[str, str],
        *,
        mode: str,
        seed: int | None = None,
        ready_timeout: float = 90,
    ) -> str:
        if self.active:
            raise RuntimeError("Thí nghiệm trước vẫn đang chạy. Hãy đợi xong hoặc dừng lab.")
        if set(assignments) != set(SENDER_IDS) or any(value not in SCENARIO_ORDER for value in assignments.values()):
            raise ValueError("Phân công phải có đúng 15 máy gửi hợp lệ.")
        if not (self.root / "models" / "live_lab" / "session_pipeline").is_dir() or not (
            self.root / "models" / "live_lab" / "metrics.json"
        ).is_file():
            raise FileNotFoundError("Chưa có mô hình Live Lab. Hãy thu traffic huấn luyện và chạy `python -m src.lab_model`.")

        experiment_id = uuid.uuid4().hex[:12]
        self._experiment_id = experiment_id
        self._stopped = False
        self._processes.clear()
        self._container_names.clear()
        self.control_dir.mkdir(parents=True, exist_ok=True)
        self.truth_dir.mkdir(parents=True, exist_ok=True)
        self.experiments_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        rows = []
        for actor_id, scenario in assignments.items():
            rows.append(
                {
                    "actor_id": actor_id,
                    "run_id": uuid.uuid4().hex,
                    "scenario": scenario,
                    "expected_flows": SCENARIO_FLOWS[scenario],
                    "status": "Đang chuẩn bị",
                }
            )
        self._write_manifest(
            {
                "experiment_id": experiment_id,
                "mode": mode,
                "seed": seed,
                "status": "preparing",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "senders": rows,
            }
        )

        try:
            self._compose("build", "generator", "victim", timeout=300)
            self._compose("up", "-d", "victim", timeout=90)
            for row in rows:
                actor_id = row["actor_id"]
                container_name = f"sparkids-{experiment_id}-{actor_id}"
                self._container_names[actor_id] = container_name
                log_path = self.logs_dir / f"{experiment_id}-{actor_id}.log"
                command = [
                    *self._compose_args(),
                    "run",
                    "--rm",
                    "--name",
                    container_name,
                    "generator",
                    row["scenario"],
                    "--actor-id",
                    actor_id,
                    "--experiment-id",
                    experiment_id,
                    "--run-id",
                    row["run_id"],
                    "--wait-for-start",
                ]
                with log_path.open("w", encoding="utf-8") as log_file:
                    self._processes[actor_id] = subprocess.Popen(
                        command,
                        cwd=self.root,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )

            self._wait_until_ready(experiment_id, ready_timeout)
            self._write_gate(experiment_id)
            manifest = self.manifest(experiment_id)
            manifest["status"] = "running"
            manifest["started_at"] = datetime.now(timezone.utc).isoformat()
            for row in manifest["senders"]:
                row["status"] = "Đang gửi"
            self._write_manifest(manifest)
            return experiment_id
        except Exception as error:
            self._stop_processes(experiment_id)
            self._remove_containers()
            manifest = self.manifest(experiment_id)
            manifest["status"] = "failed"
            manifest["error"] = str(error)
            self._write_manifest(manifest)
            raise

    def status(self, experiment_id: str | None = None) -> dict:
        experiment_id = experiment_id or self._experiment_id
        if not experiment_id:
            return {}
        manifest = self.manifest(experiment_id)
        if not manifest:
            return {}
        if manifest.get("kind") == "live":
            return self._live_status(manifest)

        # ponytail: the lab retains only a few hundred events; use checkpointed counters if runs grow large.
        observed = Counter()
        for path in self.truth_dir.glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if record.get("experiment_id") == experiment_id and record.get("actor_id"):
                observed[record["actor_id"]] += 1

        all_finished = True
        all_succeeded = True
        for row in manifest["senders"]:
            process = self._processes.get(row["actor_id"])
            return_code = process.poll() if process else None
            if self._stopped:
                if row["status"] not in {"Hoàn tất"} and not row["status"].startswith("Lỗi"):
                    row["status"] = "Đã dừng"
                all_succeeded = False
            elif return_code is None:
                row["status"] = "Đang gửi"
                all_finished = False
                all_succeeded = False
            elif return_code == 0:
                row["status"] = "Hoàn tất"
            else:
                row["status"] = f"Lỗi (mã {return_code})"
                all_succeeded = False
            row["received_flows"] = observed[row["actor_id"]]

        if all_finished and not self._stopped:
            manifest["status"] = "completed" if all_succeeded else "partial"
            manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            self._clear_gate(experiment_id)
        self._write_manifest(manifest)
        return manifest

    def latest_live_experiment_id(self) -> str | None:
        """Recover the newest interactive run when a browser or app session reloads."""
        if not self.experiments_dir.is_dir():
            return None
        candidates = []
        for path in self.experiments_dir.glob("*.json"):
            manifest = self.manifest(path.stem)
            if manifest.get("kind") != "live" or manifest.get("lab_mode") != "live":
                continue
            candidates.append((str(manifest.get("created_at", "")), path.stat().st_mtime_ns, path.stem))
        return max(candidates)[2] if candidates else None

    def stop(self, experiment_id: str | None = None) -> None:
        experiment_id = experiment_id or self._experiment_id
        if not experiment_id:
            return
        self._stopped = True
        self._clear_gate(experiment_id)
        self._stop_processes(experiment_id)
        self._remove_containers()
        manifest = self.manifest(experiment_id)
        if manifest:
            manifest["status"] = "stopped"
            manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            for row in manifest["senders"]:
                if row["status"] in {"Đang gửi", "Đang chuẩn bị"}:
                    row["status"] = "Đã dừng"
            self._write_manifest(manifest)

    def manifest(self, experiment_id: str) -> dict:
        path = self.experiments_dir / f"{experiment_id}.json"
        if not path.is_file():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _compose_args(self) -> list[str]:
        return [resolve_docker_cli(), "compose", "-p", "sparknids-live-lab", "-f", str(self.compose_file)]

    def _compose(self, *args: str, timeout: int) -> None:
        try:
            result = subprocess.run(
                [*self._compose_args(), *args],
                cwd=self.root,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise RuntimeError(f"Không chạy được Docker Compose: {error}") from error
        if result.returncode:
            detail = (result.stderr or result.stdout).strip()[-1800:]
            raise RuntimeError(f"Docker Compose thất bại: {detail}")

    def _wait_until_ready(self, experiment_id: str, timeout: float) -> None:
        expected = set(self._processes)
        deadline = time.monotonic() + timeout
        while True:
            ready = {
                path.name.split(".", 2)[1]
                for path in self.control_dir.glob(f"{experiment_id}.*.ready")
            }
            if ready == expected:
                return
            exited = [actor for actor, process in self._processes.items() if process.poll() is not None]
            if exited:
                logs = [self._read_log(actor, experiment_id) for actor in exited]
                raise RuntimeError("Máy gửi khởi động lỗi: " + " | ".join(logs))
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Chỉ {len(ready)}/15 máy sẵn sàng sau {int(timeout)} giây.")
            time.sleep(0.2)

    def _write_gate(self, experiment_id: str) -> None:
        temporary = self.control_dir / f".{experiment_id}.go.tmp"
        temporary.write_text("go", encoding="utf-8")
        temporary.replace(self.control_dir / f"{experiment_id}.go")

    def _clear_gate(self, experiment_id: str) -> None:
        (self.control_dir / f"{experiment_id}.go").unlink(missing_ok=True)
        for path in self.control_dir.glob(f"{experiment_id}.*.ready"):
            path.unlink(missing_ok=True)

    def _stop_processes(self, experiment_id: str) -> None:
        for process in self._processes.values():
            if process.poll() is None:
                process.terminate()
        for process in self._processes.values():
            if process.poll() is None:
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
        self._clear_gate(experiment_id)

    def _remove_containers(self) -> None:
        names = list(self._container_names.values())
        if names:
            try:
                subprocess.run(
                    [self._compose_args()[0], "rm", "-f", *names],
                    cwd=self.root,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass

    def _read_log(self, actor_id: str, experiment_id: str) -> str:
        path = self.logs_dir / f"{experiment_id}-{actor_id}.log"
        return path.read_text(encoding="utf-8", errors="replace")[-500:].strip() or actor_id

    def _write_manifest(self, manifest: dict) -> None:
        self.experiments_dir.mkdir(parents=True, exist_ok=True)
        path = self.experiments_dir / f"{manifest['experiment_id']}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

    def set_behavior(self, experiment_id: str, actor_id: str, profile: str, rate: float) -> None:
        manifest = self.manifest(experiment_id)
        actors = {row["actor_id"] for row in manifest.get("senders", [])}
        if not manifest or not re_valid_experiment(experiment_id) or actor_id not in actors:
            raise ValueError("Máy không thuộc lượt đang chạy.")
        if profile not in {*LIVE_LABELS, "auto"} or not math.isfinite(rate) or not 0.2 <= rate <= 25:
            raise ValueError("Chọn hành vi hợp lệ và tốc độ từ 0.2 đến 25 kết nối/giây.")
        self.control_dir.mkdir(parents=True, exist_ok=True)
        path = self.control_dir / f"{experiment_id}.{actor_id}.json"
        if profile == "auto":
            path.unlink(missing_ok=True)
            return
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"profile": profile, "rate": float(rate), "changed_at": time.time()}), encoding="utf-8")
        temporary.replace(path)
        manifest.setdefault("changes", []).append({"actor_id": actor_id, "profile": profile, "rate": rate,
            "at": datetime.now(timezone.utc).isoformat()})
        self._write_manifest(manifest)

    def start_live(self, *, duration: int = 90, seed: int = 42, automatic: bool = True,
                   lab_mode: str = "live", build: bool = True) -> str:
        if self.active:
            raise RuntimeError("Lượt trước vẫn đang gửi traffic.")
        if lab_mode not in {"live", "window_train", "window_validation", "window_test"} or not 10 <= duration <= 600:
            raise ValueError("Thời lượng hợp lệ: 10–600 giây.")
        if lab_mode == "live" and not (self.root / "models/live_lab/window_pipeline").is_dir():
            raise FileNotFoundError("Cần chuẩn bị mô hình cửa sổ trước khi chạy lab.")
        experiment_id = uuid.uuid4().hex[:12]
        self._experiment_id = experiment_id
        self._stopped = False
        self._processes.clear()
        self._container_names.clear()
        for directory in (self.control_dir, self.truth_dir, self.experiments_dir, self.logs_dir):
            directory.mkdir(parents=True, exist_ok=True)
        rows = [{"actor_id": actor, "scenario": "normal", "status": "Đang chuẩn bị"} for actor in SENDER_IDS]
        self._write_manifest({"experiment_id": experiment_id, "kind": "live", "lab_mode": lab_mode,
            "automatic": automatic, "seed": seed, "duration": duration, "status": "preparing",
            "created_at": datetime.now(timezone.utc).isoformat(), "senders": rows})
        try:
            if build:
                self._compose("build", "generator", "victim", timeout=300)
            self._compose("up", "-d", "victim", timeout=90)
            for actor in SENDER_IDS:
                container = f"sparkids-{experiment_id}-{actor}"
                self._container_names[actor] = container
                command = [*self._compose_args(), "run", "--rm", "--name", container, "generator", "live",
                    "--actor-id", actor, "--experiment-id", experiment_id, "--lab-mode", lab_mode,
                    "--duration", str(duration), "--seed", str(seed), "--wait-for-start"]
                if automatic:
                    command.append("--automatic")
                with (self.logs_dir / f"{experiment_id}-{actor}.log").open("w", encoding="utf-8") as log:
                    self._processes[actor] = subprocess.Popen(command, cwd=self.root, stdout=log,
                        stderr=subprocess.STDOUT, text=True, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            self._wait_until_ready(experiment_id, 90)
            start_at = math.ceil((time.time() + 1) / 10) * 10
            gate = self.control_dir / f"{experiment_id}.go"
            temporary = gate.with_suffix(".tmp")
            temporary.write_text(json.dumps({"start_at": start_at}), encoding="utf-8")
            temporary.replace(gate)
            manifest = self.manifest(experiment_id)
            manifest.update(status="running", started_at=datetime.fromtimestamp(start_at, timezone.utc).isoformat())
            self._write_manifest(manifest)
            return experiment_id
        except Exception as error:
            self._stop_processes(experiment_id)
            self._remove_containers()
            manifest = self.manifest(experiment_id)
            manifest.update(status="failed", error=str(error))
            self._write_manifest(manifest)
            raise

    def _live_status(self, manifest: dict) -> dict:
        experiment_id = manifest["experiment_id"]
        finished = True
        failed = False
        for row in manifest["senders"]:
            path = self.control_dir / f"{experiment_id}.{row['actor_id']}.status.json"
            try:
                row.update(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, ValueError):
                pass
            process = self._processes.get(row["actor_id"])
            code = process.poll() if process else None
            if self._stopped:
                row["status"] = "Đã dừng"
            elif process and code is None:
                row["status"] = "Đang gửi" if row.get("sent") else "Chờ bắt đầu"
                finished = False
            elif code == 0:
                row["status"] = "Hoàn tất"
            elif process:
                row["status"] = f"Lỗi ({code})"
                failed = True
            elif manifest["status"] == "running":
                row["status"] = "Không còn quản lý tiến trình"
                failed = True
        if finished and manifest["status"] == "running":
            manifest.update(status="stopped" if self._stopped else "failed" if failed else "completed",
                finished_at=datetime.now(timezone.utc).isoformat())
            self._clear_gate(experiment_id)
        self._write_manifest(manifest)
        return manifest

    def ensure_stream(self) -> None:
        progress = self.root / "outputs/live_lab/windows/progress.json"
        try:
            report = json.loads(progress.read_text(encoding="utf-8"))
            if report.get("status") == "running" and time.time() - report.get("heartbeat", 0) < 20:
                return
        except (OSError, ValueError):
            pass
        if self._stream_process and self._stream_process.poll() is None:
            return
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        python = self.root / ".venv/Scripts/python.exe"
        if not python.is_file():
            python = self.root / ".venv/bin/python"
        with (self.logs_dir / "spark-windows.log").open("a", encoding="utf-8") as log:
            self._stream_process = subprocess.Popen([str(python), "-m", "src.lab_stream"], cwd=self.root,
                stdout=log, stderr=subprocess.STDOUT, text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)

    def prepare_window_model(self) -> None:
        if self._preparation_process and self._preparation_process.poll() is None:
            return
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        report_path = self.root / "outputs/live_lab/preparation.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = report_path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"status": "starting", "updated_at": time.time()}), encoding="utf-8")
        temporary.replace(report_path)
        python = self.root / ".venv/Scripts/python.exe"
        if not python.is_file():
            python = self.root / ".venv/bin/python"
        with (self.logs_dir / "window-model-preparation.log").open("a", encoding="utf-8") as log:
            self._preparation_process = subprocess.Popen([str(python), "-m", "src.lab_window_model",
                "--collect", "--duration", "50"], cwd=self.root, stdout=log, stderr=subprocess.STDOUT,
                text=True, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)

    def start_scale(self) -> None:
        if self._scale_process and self._scale_process.poll() is None:
            return
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        python = self.root / ".venv/Scripts/python.exe"
        if not python.is_file():
            python = self.root / ".venv/bin/python"
        with (self.logs_dir / "unsw-scale-benchmark.log").open("a", encoding="utf-8") as log:
            self._scale_process = subprocess.Popen([str(python), "-m", "src.lab_scale"], cwd=self.root,
                stdout=log, stderr=subprocess.STDOUT, text=True,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)


def re_valid_experiment(value: str) -> bool:
    return len(value) == 12 and all(char in "0123456789abcdef" for char in value)

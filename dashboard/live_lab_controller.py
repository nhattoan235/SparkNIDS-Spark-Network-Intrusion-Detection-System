"""Start and supervise one isolated, synchronized Docker traffic experiment."""

from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


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
        path = self.experiments_dir / f"{manifest['experiment_id']}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

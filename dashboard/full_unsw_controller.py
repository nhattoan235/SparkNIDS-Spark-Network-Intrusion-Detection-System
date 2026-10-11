"""Start at most one background full-data Spark pipeline from the dashboard."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from dashboard.full_unsw_access import default_full_data_root
from src.spark_session import PROJECT_ROOT, configure_runtime_environment


class FullUNSWController:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root).expanduser().resolve() if root else default_full_data_root()
        self.process: subprocess.Popen | None = None
        self.log_path: Path | None = None

    @property
    def active(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self, *, current_status: str, run_id: str | None = None) -> Path:
        if self.active:
            raise RuntimeError("Pipeline UNSW đầy đủ đang chạy; đợi lượt này xong rồi hãy chạy lại.")
        configure_runtime_environment()
        self.root.mkdir(parents=True, exist_ok=True)
        logs = self.root / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
        self.log_path = logs / f"full-unsw-{stamp}.log"
        phase = "train" if current_status == "prepared" else "all"
        command = [sys.executable, "-m", "src.full_unsw_pipeline", "--phase", phase, "--root", str(self.root)]
        if phase == "train" and run_id:
            command.extend(("--run-id", run_id))
        environment = os.environ.copy()
        environment["SPARKNIDS_FULL_UNSW_ROOT"] = str(self.root)
        environment["PYSPARK_PYTHON"] = sys.executable
        environment["PYSPARK_DRIVER_PYTHON"] = sys.executable
        flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        with self.log_path.open("w", encoding="utf-8") as log:
            self.process = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                creationflags=flags,
            )
        return self.log_path

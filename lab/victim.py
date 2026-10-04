"""Measure one application-level TCP connection per incoming JSON file."""

from __future__ import annotations

import json
import os
import re
import socketserver
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


EVENT_ID = re.compile(rb"\b[0-9a-f]{32}\b")
RUN_ID = re.compile(rb"(?:X-Lab-Run|RUN_ID):\s*([0-9a-f]{32})", re.IGNORECASE)
LAB_MODE = re.compile(rb"(?:X-Lab-Mode|LAB_MODE):\s*(training|calibration|demo)", re.IGNORECASE)
ACTOR_ID = re.compile(rb"(?:X-Lab-Actor|ACTOR_ID):\s*([A-Za-z0-9-]{1,64})", re.IGNORECASE)
EXPERIMENT_ID = re.compile(rb"(?:X-Lab-Experiment|EXPERIMENT_ID):\s*([a-f0-9]{12})", re.IGNORECASE)
HTTP_REPLY = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nOK\n"
DECOY_REPLY = b"OK\n"


def _write_json(directory: Path, event_id: str, record: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f".{event_id}.tmp"
    temporary.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, directory / f"{event_id}.json")


class FlowServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, service: str, output_dir: Path):
        self.service = service
        self.output_dir = output_dir
        super().__init__(address, FlowHandler)


class FlowHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        started = time.monotonic()
        self.request.settimeout(2)
        received = bytearray()
        try:
            while len(received) < 8192:
                chunk = self.request.recv(8192 - len(received))
                if not chunk:
                    break
                received.extend(chunk)
                if b"\r\n\r\n" in received or b"\n" in received and self.server.service != "http":
                    break
        except TimeoutError:
            pass
        match = EVENT_ID.search(received)
        event_id = match.group().decode() if match else uuid.uuid4().hex
        run_match = RUN_ID.search(received)
        run_id = run_match.group(1).decode() if run_match else event_id
        mode_match = LAB_MODE.search(received)
        lab_mode = mode_match.group(1).decode().lower() if mode_match else "demo"
        actor_match = ACTOR_ID.search(received)
        experiment_match = EXPERIMENT_ID.search(received)
        reply = HTTP_REPLY if self.server.service == "http" else DECOY_REPLY
        record = {
            "event_id": event_id,
            "run_id": run_id,
            "actor_id": actor_match.group(1).decode() if actor_match else None,
            "experiment_id": experiment_match.group(1).decode() if experiment_match else None,
            "lab_mode": lab_mode,
            "record_type": "flow",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_ip": self.client_address[0],
            "source_port": self.client_address[1],
            "destination_port": self.server.server_address[1],
            "proto": "tcp",
            "service": self.server.service,
            "dur": max(time.monotonic() - started, 1e-6),
            "sbytes": len(received),
            "dbytes": len(reply),
        }
        # The scenario label is kept in a separate directory and never enters the model input.
        _write_json(self.server.output_dir / lab_mode, event_id, record)
        try:
            self.request.sendall(reply)
        except OSError:
            pass


def make_server(port: int, service: str, output_dir: Path, host: str = "127.0.0.1") -> FlowServer:
    if service not in {"http", "-"}:
        raise ValueError("service must be http or -")
    return FlowServer((host, port), service, Path(output_dir))


def main() -> None:
    output_dir = Path("/lab/incoming")
    servers = [make_server(8080, "http", output_dir, host="0.0.0.0")]
    servers.extend(make_server(port, "-", output_dir, host="0.0.0.0") for port in range(9001, 9013))
    for server in servers:
        threading.Thread(target=server.serve_forever, daemon=True).start()
    print("Lab victim ready: HTTP 8080, decoy TCP 9001-9012", flush=True)
    try:
        threading.Event().wait()
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    main()

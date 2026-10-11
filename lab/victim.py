"""Measure one application-level TCP connection per incoming JSON file."""

from __future__ import annotations

import json
import os
import re
import queue
import socketserver
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


EVENT_ID = re.compile(rb"\b[0-9a-f]{32}\b")
RUN_ID = re.compile(rb"(?:X-Lab-Run|RUN_ID):\s*([0-9a-f]{32})", re.IGNORECASE)
LAB_MODE = re.compile(rb"(?:X-Lab-Mode|LAB_MODE):\s*(window_validation|window_train|window_test|training|calibration|demo|live)", re.IGNORECASE)
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

    def __init__(self, address, service: str, output_dir: Path, writer=None):
        self.service = service
        self.output_dir = output_dir
        self.writer = writer
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
        observed_at = datetime.now(timezone.utc).isoformat()
        reply = (HTTP_REPLY.replace(b"Connection: close", f"X-Lab-Observed-At: {observed_at}\r\nConnection: close".encode())
            if self.server.service == "http" else f"OK TIME {observed_at}\n".encode())
        record = {
            "event_id": event_id,
            "run_id": run_id,
            "actor_id": actor_match.group(1).decode() if actor_match else None,
            "experiment_id": experiment_match.group(1).decode() if experiment_match else None,
            "lab_mode": lab_mode,
            "record_type": "flow",
            "timestamp": observed_at,
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
        if self.server.writer:
            self.server.writer.put(record)
        else:
            _write_json(self.server.output_dir / lab_mode, event_id, record)
        try:
            self.request.sendall(reply)
        except OSError:
            pass


def make_server(port: int, service: str, output_dir: Path, host: str = "127.0.0.1", writer=None) -> FlowServer:
    if service not in {"http", "-"}:
        raise ValueError("service must be http or -")
    return FlowServer((host, port), service, Path(output_dir), writer=writer)


def flush_batches(events: queue.Queue, output_dir: Path) -> None:
    """Publish one atomic JSONL file per mode per second, instead of tiny files."""
    while True:
        time.sleep(1)
        batches = {}
        while True:
            try:
                record = events.get_nowait()
            except queue.Empty:
                break
            batches.setdefault(record["lab_mode"], []).append(record)
        for mode, records in batches.items():
            directory = output_dir / mode
            directory.mkdir(parents=True, exist_ok=True)
            batch_id = uuid.uuid4().hex
            temporary = directory / f".{batch_id}.tmp"
            temporary.write_text("\n".join(json.dumps(row) for row in records) + "\n", encoding="utf-8")
            os.replace(temporary, directory / f"{batch_id}.json")


def main() -> None:
    output_dir = Path("/lab/incoming")
    events = queue.Queue(maxsize=20000)
    threading.Thread(target=flush_batches, args=(events, output_dir), daemon=True).start()
    servers = [make_server(8080, "http", output_dir, host="0.0.0.0", writer=events)]
    servers.extend(make_server(port, "-", output_dir, host="0.0.0.0", writer=events) for port in range(9001, 9013))
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

"""Small display snapshots and independent truth checks for the classroom viewer."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from src.lab_features import WINDOW_SECONDS


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {} if default is None else default


def render_scale_chart(chart, frame):
    chart.bar_chart(
        frame,
        x="Quy mô dữ liệu",
        y="Tốc độ Spark (flow/giây)",
        color="Partition",
        x_label="Số flow Spark đọc",
        y_label="Flow được xử lý trong một giây",
    )


def spark_throughput_rows(batches: list[dict]) -> list[dict]:
    rows = []
    for batch in batches:
        input_rows = int(batch.get("input_rows", batch.get("numInputRows", 0)) or 0)
        duration_seconds = float(batch.get(
            "duration_seconds",
            batch.get("batch_seconds", batch.get("durationMs", {}).get("triggerExecution", 0) / 1000),
        ) or 0)
        if input_rows <= 0 or duration_seconds <= 0:
            continue
        rate = batch.get("flow_per_second")
        rate = float(rate) if rate is not None else input_rows / duration_seconds
        batch_id = batch.get("batch_id", batch.get("stream_batch_id", batch.get("batchId", 0)))
        rows.append({
            "Lô Spark": f"#{int(batch_id)}",
            "Flow/giây": rate,
            "Flow trong lô": input_rows,
            "Thời gian xử lý (giây)": duration_seconds,
        })
    return rows


def remember_spark_batch(history: list[dict], batch: dict) -> list[dict]:
    input_rows = int(batch.get("input_rows", 0) or 0)
    duration = float(batch.get("batch_seconds", 0) or 0)
    batch_id = int(batch.get("batch_id", batch.get("stream_batch_id", 0)) or 0)
    if input_rows <= 0 or duration <= 0 or any(item.get("batch_id") == batch_id for item in history):
        return history
    history.append({"batch_id": batch_id, "input_rows": input_rows, "batch_seconds": duration})
    del history[:-30]
    return history


def window_count_explanation(window_count: int, sender_count: int, windows_per_machine: int) -> str:
    if window_count <= 0:
        return f"Chưa có cửa sổ dữ liệu {WINDOW_SECONDS} giây để Spark chấm."
    if sender_count > 0 and windows_per_machine > 0 and window_count == sender_count * windows_per_machine:
        return (
            f"{window_count} lần chấm = {sender_count} máy × {windows_per_machine} cửa sổ/máy; "
            f"mỗi cửa sổ gom kết nối trong {WINDOW_SECONDS} giây, không phải một kết nối."
        )
    return (
        f"{window_count} lần chấm; mỗi lần là dữ liệu của một máy trong một cửa sổ "
        f"{WINDOW_SECONDS} giây, không phải một kết nối."
    )


def stream_feedback(experiment_status, stream_fresh, checked_count, total_count):
    if stream_fresh:
        return "đang xử lý", "Spark đang xử lý các flow mới."
    if experiment_status == "completed" and total_count > 0 and checked_count == total_count:
        return "đã chốt", (
            f"Spark đã đối chiếu đủ {checked_count} cửa sổ dữ liệu "
            f"(mỗi cửa sổ là 1 máy trong {WINDOW_SECONDS} giây)."
        )
    if experiment_status == "completed":
        waiting = max(total_count - checked_count, 0)
        return "chờ đối chiếu", f"Máy gửi đã xong; còn {waiting} cửa sổ chưa đối chiếu được."
    return "tạm dừng", "Spark đang tạm dừng. Bắt đầu lượt mô phỏng để khởi động lại luồng xử lý."


def epoch(value: str) -> float:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


class TruthIndex:
    """Incrementally read external labels; never inject them into model features."""
    def __init__(self):
        self.seen = set()
        self.offsets = {}
        self.windows = defaultdict(lambda: {"label": 0, "count": 0, "scenarios": set()})

    def update(self, directory: Path):
        for path in directory.glob("*.json*"):
            if path.suffix == ".jsonl":
                with path.open("r", encoding="utf-8") as stream:
                    stream.seek(self.offsets.get(path.name, 0))
                    while True:
                        offset = stream.tell()
                        line = stream.readline()
                        if not line or not line.endswith("\n"):
                            self.offsets[path.name] = offset
                            break
                        self._add(json.loads(line))
                        self.offsets[path.name] = stream.tell()
            elif path.name not in self.seen:
                self._add(read_json(path))
                self.seen.add(path.name)

    def _add(self, row: dict):
        if not row.get("timestamp") or not row.get("source_ip"):
            return
        start = int(epoch(row["timestamp"]) // WINDOW_SECONDS) * WINDOW_SECONDS
        key = (row["source_ip"], start)
        window = self.windows[key]
        window["count"] += 1
        window["label"] = max(window["label"], int(row["label"]))
        window["scenarios"].add(row["scenario"])

    def check(self, row: dict, watermark: str | None, *, run_complete: bool = False) -> dict:
        observed = self.windows.get((row["source_ip"], int(epoch(row["window_start"]))))
        final = run_complete or bool(watermark and epoch(watermark) >= epoch(row["window_end"]))
        if not final:
            return {"checked": False, "outcome": "Tạm tính", "actual": None}
        if not observed or observed["count"] != int(row["flow_count"]):
            return {"checked": False, "outcome": "Chờ đủ dữ liệu đối chiếu", "actual": None}
        actual = observed["label"]
        prediction = int(row["prediction"])
        outcome = {(1, 1): "Bắt đúng", (0, 1): "Báo nhầm", (1, 0): "Bỏ sót", (0, 0): "Bình thường đúng"}[(actual, prediction)]
        return {"checked": True, "outcome": outcome, "actual": actual}


def network_svg(senders: list[dict], latest: dict) -> str:
    """Topology encodes measured decisions, not assigned attack labels."""
    pieces = ['<svg viewBox="0 0 800 300" role="img" aria-label="15 máy gửi tới máy chủ, dữ liệu qua Spark và MLlib" style="width:100%;height:auto">']
    pieces.append('<path d="M 410 42 V 242 M 410 142 H 462" fill="none" stroke="#b8c0ba" stroke-width="2"/>')
    for index, sender in enumerate(senders):
        x, y = 8 + (index % 5) * 77, 28 + (index // 5) * 88
        result = latest.get(sender["actor_id"])
        color = "#ed5b24" if result and result["prediction"] else "#17624b" if result else "#66716d"
        text = "Nghi ngờ" if result and result["prediction"] else "Bình thường" if result else "Chờ Spark"
        pieces.append(f'<g><rect x="{x}" y="{y}" width="69" height="53" rx="5" fill="#fbfaf6" stroke="{color}" stroke-width="2"/><text x="{x+7}" y="{y+20}" fill="#14201e" font-size="12" font-weight="700">Máy {index+1:02d}</text><text x="{x+7}" y="{y+39}" fill="{color}" font-size="9">{escape(text)}</text></g>')
    for y in (54, 142, 230):
        pieces.append(f'<path d="M 391 {y} H 410" stroke="#b8c0ba" stroke-width="2"/>')
    for x, title, detail in ((463, "WEB SERVER", "Nhận TCP thật"), (463, "APACHE SPARK", "Gom IP / 10 giây"), (634, "MLLIB", "Chấm điểm")):
        y = 52 if title == "WEB SERVER" else 183
        pieces.append(f'<rect x="{x}" y="{y}" width="149" height="64" rx="6" fill="#14201e"/><text x="{x+13}" y="{y+26}" fill="#fbfaf6" font-size="13" font-weight="700">{title}</text><text x="{x+13}" y="{y+47}" fill="#dfe2dc" font-size="11">{detail}</text>')
    pieces.append('<path d="M 537 116 V 178 M 612 215 H 629" stroke="#ed5b24" stroke-width="3" fill="none"/><text x="551" y="151" fill="#66716d" font-size="11">Flow mới</text></svg>')
    return "".join(pieces)

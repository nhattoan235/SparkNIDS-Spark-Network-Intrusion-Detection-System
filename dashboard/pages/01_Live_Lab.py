"""Run 15 synchronized senders in the isolated Docker IDS lab."""

from __future__ import annotations

import json
import secrets
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.live_lab_controller import (
    SCENARIO_FLOWS,
    SCENARIO_NAMES,
    SENDER_IDS,
    LiveLabController,
    build_assignments,
    should_offer_stop,
)


st.set_page_config(page_title="Live Lab · Spark IDS", page_icon="🛡️", layout="wide")
st.title("Live Lab · 15 máy gửi traffic")
st.caption("15 container tạo traffic trong Docker riêng → Spark gom dữ liệu → MLlib chấm từng lượt; đây không phải 15 máy Spark vật lý.")

controller = st.session_state.setdefault("live_lab_controller", LiveLabController(ROOT))
model_report_path = ROOT / "models" / "live_lab" / "metrics.json"
model_ready = model_report_path.is_file() and (ROOT / "models" / "live_lab" / "session_pipeline").is_dir()

if not model_ready:
    st.warning("Chưa có mô hình Live Lab. Thu traffic huấn luyện và chạy python -m src.lab_model trước khi demo.")

mode = st.radio(
    "Cách chia kịch bản",
    ["Tự động", "Tự chọn"],
    horizontal=True,
    help="Cả hai chế độ đều chạy đúng 15 máy; luôn có ít nhất một máy bình thường để làm mốc.",
)
seed = st.session_state.setdefault("live_lab_seed", secrets.randbits(32))
counts: dict[str, int] | None = None

if mode == "Tự động":
    seed_column, shuffle_column = st.columns([1, 4])
    seed_column.metric("Seed xáo trộn", seed)
    if shuffle_column.button("Xáo lại thứ tự máy"):
        st.session_state["live_lab_seed"] = secrets.randbits(32)
        st.rerun()
    st.caption("Tự chia cân bằng: 5 bình thường · 5 quét cổng · 5 gửi dồn. Seed giúp lưu lại đúng lần chia này.")
    assignments = build_assignments("Tự động", seed)
else:
    normal_col, scan_col, burst_col = st.columns(3)
    counts = {
        "normal": normal_col.number_input("Máy bình thường", 1, 15, 5, key="live_normal_count"),
        "scan": scan_col.number_input("Máy quét cổng", 0, 15, 5, key="live_scan_count"),
        "burst": burst_col.number_input("Máy gửi dồn", 0, 15, 5, key="live_burst_count"),
    }
    valid_counts = sum(counts.values()) == len(SENDER_IDS)
    if not valid_counts:
        st.warning(f"Hiện có {sum(counts.values())}/15 máy. Điều chỉnh để tổng đúng 15.")
    assignments = build_assignments("Tự chọn", seed, counts) if valid_counts else {}

descriptions = {
    "normal": "12 yêu cầu HTTP, cách nhau",
    "scan": "12 cổng mồi 9001–9012",
    "burst": "30 yêu cầu HTTP gửi đồng thời",
}
preview = pd.DataFrame(
    [
        {
            "Máy gửi": actor_id.replace("sender-", "Máy "),
            "Kịch bản": SCENARIO_NAMES[scenario],
            "Máy sẽ làm gì": descriptions[scenario],
            "Số kết nối": SCENARIO_FLOWS[scenario],
        }
        for actor_id, scenario in assignments.items()
    ]
)
total_flows = int(preview["Số kết nối"].sum()) if not preview.empty else 0
st.dataframe(preview, width="stretch", hide_index=True, height=390)
st.caption(f"Tổng dự kiến: {len(preview)}/15 máy · {total_flows} kết nối. Traffic chạy trong Docker nội bộ, không gửi ra Internet.")

@st.fragment(run_every="2s")
def render_run_controls() -> None:
    active = controller.active
    experiment_id = st.session_state.get("live_lab_experiment_id")
    stop_slot = st.empty()
    if active:
        st.info("Một lượt mô phỏng vẫn đang chạy. Chờ hoàn tất hoặc dừng trước khi tạo lượt mới.")
    if experiment_id and should_offer_stop(controller.manifest(experiment_id), active):
        if stop_slot.button("Dừng lượt mô phỏng", key=f"stop_{experiment_id}"):
            controller.stop(experiment_id)
            st.rerun()
    else:
        stop_slot.empty()
    if st.button(
        "Chạy mô phỏng 15 máy",
        type="primary",
        disabled=active or not model_ready or not assignments,
    ):
        try:
            with st.spinner("Đang chuẩn bị Docker và chờ đủ cả 15 máy sẵn sàng…"):
                experiment_id = controller.start_experiment(
                    assignments,
                    mode=mode,
                    seed=seed if mode == "Tự động" else None,
                )
            st.session_state["live_lab_experiment_id"] = experiment_id
            st.rerun()
        except Exception as error:
            st.error(str(error))


render_run_controls()


def read_prediction_rows() -> pd.DataFrame:
    prediction_dir = ROOT / "outputs" / "live_lab" / "sessions" / "predictions"
    paths = sorted(prediction_dir.glob("*.json")) if prediction_dir.is_dir() else []
    frames = []
    for path in paths:
        try:
            frames.append(pd.read_json(path, lines=True))
        except (OSError, ValueError):
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def read_truth_rows(experiment_id: str) -> dict[str, dict]:
    run_dir = ROOT / "data" / "lab" / "truth" / "runs"
    result = {}
    if run_dir.is_dir():
        for path in run_dir.glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if record.get("experiment_id") == experiment_id and record.get("run_id"):
                result[record["run_id"]] = record
    return result


def read_live_metrics() -> dict:
    try:
        return json.loads(model_report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


@st.fragment(run_every="2s")
def render_experiment(experiment_id: str) -> None:
    manifest = controller.status(experiment_id)
    if not manifest:
        st.warning("Không tìm thấy thông tin lượt mô phỏng này.")
        return

    rows = manifest.get("senders", [])
    completed = sum(row["status"] == "Hoàn tất" for row in rows)
    sent_total = sum(row.get("received_flows", 0) for row in rows)
    predictions = read_prediction_rows()
    by_run = {}
    if not predictions.empty and "run_id" in predictions:
        by_run = {str(row["run_id"]): row for _, row in predictions.iterrows()}
    truths = read_truth_rows(experiment_id)
    current_runs = {row["run_id"] for row in rows}
    scored = {run_id: row for run_id, row in by_run.items() if run_id in current_runs}
    alert_count = sum(int(item["prediction"]) == 1 for item in scored.values())
    outcomes = {"Bắt đúng": 0, "Báo động nhầm": 0, "Bỏ sót": 0, "Bình thường đúng": 0}
    for sender in rows:
        prediction = by_run.get(sender["run_id"])
        truth = truths.get(sender["run_id"], {})
        if prediction is None or not truth or sender.get("received_flows", 0) < sender["expected_flows"]:
            continue
        if int(prediction["flow_count"]) < sender["expected_flows"]:
            continue
        actual = int(truth["label"])
        predicted = int(prediction["prediction"])
        if actual == predicted == 1:
            outcomes["Bắt đúng"] += 1
        elif actual == 0 and predicted == 1:
            outcomes["Báo động nhầm"] += 1
        elif actual == 1 and predicted == 0:
            outcomes["Bỏ sót"] += 1
        else:
            outcomes["Bình thường đúng"] += 1

    a, b, c, d = st.columns(4)
    a.metric("Máy gửi xong", f"{completed}/15")
    b.metric("Kết nối đã nhận", f"{sent_total:,}/{sum(row['expected_flows'] for row in rows):,}")
    c.metric("Lượt Spark đã chấm", f"{len(scored)}/15")
    d.metric("MLlib gắn cờ", alert_count)
    st.caption(f"Mã lượt: {experiment_id} · Seed: {manifest.get('seed') or 'tự chọn'} · Tự cập nhật mỗi 2 giây")

    display_rows = []
    for row in rows:
        actor_id = row["actor_id"]
        run_id = row["run_id"]
        prediction = by_run.get(run_id)
        truth = truths.get(run_id, {})
        flow_count = int(prediction["flow_count"]) if prediction is not None and pd.notna(prediction.get("flow_count")) else 0
        expected = int(row["expected_flows"])
        label = int(truth["label"]) if "label" in truth else None
        received = row.get("received_flows", 0)
        complete = flow_count >= expected and received >= expected
        if prediction is None:
            decision, probability, assessment = "Chưa có", "—", "Chờ Spark xử lý"
        else:
            predicted = int(prediction["prediction"])
            decision = "Đáng ngờ" if predicted else "Bình thường"
            probability = f"{float(prediction['attack_probability']):.3f}"
            if not complete:
                decision = f"Tạm thời: {decision}"
                assessment = "Thiếu kết nối" if row["status"] == "Hoàn tất" and received < expected else "Đang gom flow"
            elif label is None:
                assessment = "Thiếu nhãn đối chiếu"
            elif predicted == label:
                assessment = "Bắt đúng" if label else "Bình thường nhận đúng"
            else:
                assessment = "Bỏ sót" if label else "Báo động nhầm"
        display_rows.append(
            {
                "Máy": actor_id.replace("sender-", "Máy "),
                "Kịch bản thật": SCENARIO_NAMES[row["scenario"]],
                "Trạng thái gửi": row["status"],
                "Máy gửi nhận": f"{received}/{expected}",
                "Spark gom": f"{flow_count}/{expected}",
                "MLlib dự đoán": decision,
                "Điểm nghi ngờ": probability,
                "Đối chiếu": assessment,
            }
        )
    st.dataframe(pd.DataFrame(display_rows), width="stretch", hide_index=True)

    if sum(outcomes.values()):
        st.markdown("**Đối chiếu các máy đã đủ kết nối**")
        first, second, third, fourth = st.columns(4)
        first.metric("Bắt đúng", outcomes["Bắt đúng"])
        second.metric("Báo động nhầm", outcomes["Báo động nhầm"])
        third.metric("Bỏ sót", outcomes["Bỏ sót"])
        fourth.metric("Bình thường nhận đúng", outcomes["Bình thường đúng"])
    else:
        st.caption("Các số đối chiếu chỉ hiện sau khi Spark nhận đủ flow của máy.")

    failed = [row for row in rows if row["status"].startswith("Lỗi")]
    if failed:
        st.error("Một số máy gửi lỗi. Mở log tương ứng trong outputs/live_lab/actors để xem chi tiết.")
    if manifest.get("error"):
        st.error(manifest["error"])
    if len(scored) < len(rows):
        st.info("Nếu Spark chưa chấm: mở Spark stream bằng python -m src.lab_stream, rồi đợi bảng cập nhật.")

    evaluation = read_live_metrics().get("lab_sessions", {})
    familiar = evaluation.get("test")
    challenge = evaluation.get("challenge_test")
    if familiar:
        with st.expander("Bài kiểm tra mô hình: mẫu quen và mẫu khác", expanded=True):
            left, right = st.columns(2)
            left.metric(
                "Cùng kiểu traffic đã học",
                f"{familiar['true_positive'] + familiar['true_negative']}/{familiar['total']} đúng",
            )
            left.caption(f"{familiar['total']} lượt Docker giữ riêng, cùng ba mẫu cố định.")
            if challenge:
                right.metric(
                    "Traffic khác cấu hình",
                    f"{challenge['true_positive'] + challenge['true_negative']}/{challenge['total']} đúng",
                )
                right.caption(f"{challenge['total']} lượt Docker khác nhịp, không dùng để huấn luyện mô hình này.")
                normal_total = challenge["true_negative"] + challenge["false_positive"]
                if normal_total:
                    st.warning(f"Bài khác cấu hình: báo nhầm {challenge['false_positive']}/{normal_total} lượt bình thường.")
            else:
                right.info("Chưa có bài thử khác cấu hình. Chạy python -m src.lab_model --audit-only.")
            st.caption("Hai kết quả này đo mô hình trên dữ liệu Docker đã ghi; bảng 15 máy phía trên là lượt đang xem.")

experiment_id = st.session_state.get("live_lab_experiment_id")
if experiment_id:
    st.divider()
    st.subheader("Kết quả lượt mô phỏng")
    st.caption("Lượt 15 máy dùng ba kịch bản cố định. Bài thử khác cấu hình ở cuối trang cho thấy mô hình chịu thay đổi ra sao.")
    render_experiment(experiment_id)

summary_path = ROOT / "outputs" / "live_lab" / "sessions" / "summaries" / "latest.json"


@st.fragment(run_every="5s")
def render_spark_status() -> None:
    if summary_path.is_file():
        age = time.time() - summary_path.stat().st_mtime
        if age < 30:
            st.success("Spark vừa xử lý dữ liệu")
        else:
            st.info("Đang chờ flow mới; lượt trước đã được xử lý.")
    else:
        st.warning("Chưa có báo cáo xử lý từ Spark")


with st.sidebar:
    render_spark_status()


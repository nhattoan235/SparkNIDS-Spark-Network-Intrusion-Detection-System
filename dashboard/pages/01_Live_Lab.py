"""Interactive, honest Docker traffic console for the Spark IDS project."""

from __future__ import annotations

import json
import math
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.live_lab_controller import SENDER_IDS, LiveLabController
from dashboard.live_view import (
    TruthIndex,
    network_svg,
    read_json,
    remember_spark_batch,
    render_scale_chart,
    spark_throughput_rows,
    stream_feedback,
    window_count_explanation,
)
from dashboard.styles import inject_styles
from dashboard.ui import metric_cards_html
from lab.generator import LIVE_LABELS, LIVE_NAMES, LIVE_RATES
from src.lab_window_model import MODEL_FEATURES


st.set_page_config(page_title="Lab mạng · Spark IDS", page_icon="◉", layout="wide")
inject_styles()
st.markdown(
    """
    <style>
    .block-container {max-width:1340px; padding-top:1.2rem}
    .network-board {background:#fbfaf6;border:1px solid #b8c0ba;padding:.7rem;margin:.4rem 0}
    .network-board svg {display:block;width:100%;height:auto}
    .live-lede {font-size:1rem;line-height:1.45;max-width:900px;color:#46514e;margin-top:-.35rem}
    .tag {font-size:.72rem;letter-spacing:.12em;color:#ed5b24;font-weight:800}
    .scenario-strip {display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:.55rem;margin:.55rem 0 1rem}
    .scenario-step {padding:.65rem .75rem;background:#fbfaf6;border:1px solid #b8c0ba;border-top:3px solid #17624b}
    .scenario-step.alert {border-top-color:#ed5b24}
    .scenario-step small,.scenario-step strong,.scenario-step span {display:block}
    .scenario-step small {font-size:.68rem;color:#66716d;letter-spacing:.04em}
    .scenario-step strong {margin:.15rem 0;font-size:.86rem;color:#14201e}
    .scenario-step span {font-size:.75rem;line-height:1.3;color:#66716d}
    .phase-line {font-size:1rem;font-weight:700;color:#14201e;margin:.15rem 0 .45rem}
    @media (max-width:650px) {.scenario-strip {grid-template-columns:1fr}}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def shared_controller(root: str):
    return LiveLabController(Path(root))


@st.cache_resource
def truth_index(experiment: str):
    return TruthIndex()


controller = shared_controller(str(ROOT))
st.session_state.setdefault("interactive_experiment", None)
experiment_id = st.session_state.get("interactive_experiment") or controller.latest_live_experiment_id()
if experiment_id:
    st.session_state["interactive_experiment"] = experiment_id
st.markdown('<div class="tag">DEMO THỰC TẾ · APACHE SPARK + MLLIB</div>', unsafe_allow_html=True)
st.title("15 máy gửi kết nối. Spark tìm máy bất thường.")
st.markdown(
    '<p class="live-lede">Máy chủ Docker ghi nhận kết nối thật. Spark gom theo từng máy mỗi 10 giây, '
    'sau đó MLlib chấm điểm để phát hiện hành vi đáng ngờ.</p>',
    unsafe_allow_html=True,
)
st.caption("Traffic = các kết nối mạng do 15 máy Docker gửi; mỗi kết nối được ghi thành một flow. Demo chạy trong mạng Docker nội bộ.")

model_dir = ROOT / "models/live_lab/window_pipeline"
metrics_path = ROOT / "models/live_lab/window_metrics.json"
preparation = read_json(ROOT / "outputs/live_lab/preparation.json")
prepared = model_dir.is_dir() and metrics_path.is_file()

if not prepared:
    @st.fragment(run_every="2s")
    def render_preparation():
        current = read_json(ROOT / "outputs/live_lab/preparation.json")
        active = current.get("status") in {"starting", "collecting", "training"}
        left, right = st.columns([3, 2])
        with left:
            st.warning("Mô hình cửa sổ cần ba lượt Docker riêng để huấn luyện và kiểm tra trên lượt giữ riêng.")
        with right:
            if st.button("Chuẩn bị mô hình bằng Docker", type="primary", disabled=active):
                try:
                    controller.prepare_window_model()
                    st.rerun()
                except Exception as error:
                    st.error(str(error))
        if active:
            st.info(f"Đang chuẩn bị: {current.get('split', current['status'])}. Lần chạy này mất vài phút.")
        elif current.get("status") == "failed":
            st.error(f"Chuẩn bị chưa thành công: {current.get('error', 'xem log')}")
            st.caption("Log chi tiết nằm trong outputs/live_lab/actors/window-model-preparation.log.")
        st.caption("Mỗi bộ có 15 sender; mô hình không nhận nhãn hoặc tên kịch bản.")
    render_preparation()
    st.stop()


@st.fragment(run_every="2s")
def render_console():
    global experiment_id
    manifest = controller.status(experiment_id) if experiment_id else {}
    progress = read_json(ROOT / "outputs/live_lab/windows/progress.json")
    current_metrics = read_json(metrics_path)
    truth = truth_index(experiment_id) if experiment_id else None
    active = bool(manifest and manifest.get("status") in {"running", "preparing"})

    if manifest:
        if truth:
            truth.update(ROOT / "data/lab/truth/events" / experiment_id)
        st.markdown("### Lượt mô phỏng")
    else:
        st.markdown("### Bắt đầu mô phỏng")

    if manifest.get("status") == "preparing":
        st.info("Đang chuẩn bị 15 máy Docker. Đồng hồ bắt đầu khi tất cả máy sẵn sàng gửi.")
    elif manifest.get("status") == "running":
        run_duration = int(manifest.get("duration", 90))
        try:
            started_at = datetime.fromisoformat(manifest["started_at"]).timestamp()
        except (KeyError, TypeError, ValueError):
            started_at = time.time()
        elapsed = max(0.0, time.time() - started_at)
        remaining = max(0, math.ceil(run_duration - elapsed))
        minutes, seconds = divmod(remaining, 60)
        done = min(1.0, elapsed / run_duration)
        timer_col, phase_col = st.columns([1, 3])
        timer_col.metric("Thời gian còn lại", f"{minutes:02d}:{seconds:02d}")
        if manifest.get("automatic", True):
            if elapsed < 15:
                phase = "Giai đoạn 1/3 · Máy gửi bình thường để làm mốc"
            elif elapsed < 60:
                phase = "Giai đoạn 2/3 · 7 máy gửi hành vi bất thường"
            else:
                phase = "Giai đoạn 3/3 · Máy trở lại bình thường"
        else:
            phase = "Đang chạy · Bạn có thể đổi hành vi từng máy bên dưới"
        phase_col.markdown(f'<div class="phase-line">{phase}</div>', unsafe_allow_html=True)
        phase_col.progress(done, text="Thời gian gửi traffic")
        if remaining == 0:
            st.info("Đã hết thời gian gửi. Đang chờ các máy kết thúc lượt gửi.")
    elif manifest.get("status") == "completed":
        st.info("Các máy đã gửi xong. Kết quả mới nhất của lượt này nằm ở phần bên dưới.")
    elif manifest.get("status") == "stopped":
        st.warning("Lượt trước đã dừng giữa chừng. Số liệu bên dưới chỉ tính đến lúc dừng; bấm “Chạy 15 máy cùng lúc” để tạo lượt mới.")
    elif manifest.get("status") == "failed":
        st.error(f"Lượt chạy chưa hoàn tất: {manifest.get('error', 'xem trạng thái Docker')}")

    mode, settings = st.columns([1, 2])
    with mode:
        automatic_default = bool(manifest.get("automatic", True)) if active else True
        automatic = st.toggle("Tự chạy 3 giai đoạn", value=automatic_default, disabled=active)
        duration_default = int(manifest.get("duration", 90)) if active else 90
        duration = st.select_slider("Thời gian chạy", options=[75, 90, 120, 180], value=duration_default,
            format_func=lambda value: f"{value} giây", disabled=active)
        if active:
            st.session_state["live_seed"] = int(manifest.get("seed", 42))
        else:
            st.session_state.setdefault("live_seed", 42)
        with st.expander("Tùy chọn thêm", expanded=False):
            seed = st.number_input("Mã xáo hành vi", min_value=1, max_value=999_999, step=1,
                key="live_seed", disabled=active)
            st.caption("Giữ mã 42 để phân công hành vi giống lượt trước.")

        if automatic:
            st.markdown(
                f'<div class="scenario-strip">'
                f'<div class="scenario-step"><small>00:00–00:15</small><strong>Bình thường</strong><span>Làm mốc so sánh</span></div>'
                f'<div class="scenario-step alert"><small>00:15–01:00</small><strong>7 máy bất thường</strong><span>Quét cổng hoặc gửi dồn</span></div>'
                f'<div class="scenario-step"><small>01:00–{duration // 60:02d}:{duration % 60:02d}</small><strong>Trở lại bình thường</strong><span>Xem cảnh báo có hạ xuống không</span></div>'
                f'</div>', unsafe_allow_html=True)
        else:
            st.caption("Các máy gửi bình thường. Mở phần điều khiển bên dưới để đổi hành vi.")

        if st.button("Chạy 15 máy cùng lúc", type="primary", disabled=active):
            try:
                controller.ensure_stream()
                experiment_id = controller.start_live(duration=int(duration), seed=int(seed), automatic=automatic, build=True)
                st.session_state["interactive_experiment"] = experiment_id
                st.rerun()
            except Exception as error:
                st.error(str(error))
        if active:
            if st.button("Dừng gửi traffic"):
                controller.stop(experiment_id)
                st.rerun()
    with settings:
        if manifest and manifest.get("senders"):
            senders = manifest["senders"]
            st.markdown(
                f'<div class="network-board">{network_svg(senders, latest_by_actor(read_windows(experiment_id)))}</div>',
                unsafe_allow_html=True)
            st.caption("Cam: Spark nghi ngờ · Xanh: Spark nhận định bình thường · Xám: đang chờ dữ liệu.")
        else:
            st.info("Sơ đồ 15 máy sẽ cập nhật khi lượt chạy bắt đầu.")

    if manifest and manifest.get("kind") == "live":
        with st.expander("Điều khiển hành vi của từng máy", expanded=False):
            st.caption("Dùng phần này nếu muốn tự đổi hành vi trong lúc mô phỏng đang chạy.")
            names = {actor: f"Máy {index + 1:02d}" for index, actor in enumerate(SENDER_IDS)}
            selected_actor = st.pills("Chọn máy", SENDER_IDS, default=SENDER_IDS[0], format_func=lambda actor: names[actor], key="live_actor")
            choice_names = {"auto": "Theo kịch bản", **{key: value for key, value in LIVE_NAMES.items()}}
            selected_profile = st.selectbox("Chọn hành vi", list(choice_names), format_func=lambda key: choice_names[key], key="live_profile")
            rate = st.slider("Tốc độ gửi", 0.2, 25.0, float(LIVE_RATES.get(selected_profile, 1.0)), 0.2,
                format="%.1f kết nối/giây", key="live_rate")
            if selected_actor and st.button("Áp dụng cho máy đã chọn", disabled=manifest.get("status") not in {"running", "preparing"}):
                try:
                    controller.set_behavior(experiment_id, selected_actor, selected_profile, rate)
                    st.toast(f"Đã đổi {names[selected_actor]} thành {choice_names[selected_profile]}")
                except Exception as error:
                    st.error(str(error))
            st.caption("Quét cổng chỉ chạm 12 cổng mồi trong Docker nội bộ.")
    else:
        st.caption("Sau khi bắt đầu lượt chạy, bạn có thể mở phần điều khiển để đổi hành vi một máy.")

    if manifest and manifest.get("senders"):
        rows = read_windows(experiment_id)
        latest = latest_by_actor(rows)
        watermark = progress.get("event_time", {}).get("watermark")
        checked_by_window = {}
        outcomes = Counter()
        run_complete = manifest.get("status") == "completed"
        if truth:
            for row in rows:
                result = truth.check(row, watermark, run_complete=run_complete)
                checked_by_window[(row["source_ip"], row["window_start"])] = result
                if result.get("checked"):
                    outcomes[result["outcome"]] += 1
        checked = {
            actor: checked_by_window.get((row["source_ip"], row["window_start"]),
                {"checked": False, "outcome": "Tạm tính", "actual": None})
            for actor, row in latest.items()
        }
        windows_by_actor = Counter(row.get("actor_id") for row in rows)
        windows_per_machine = next(iter(windows_by_actor.values()), 0) if len(set(windows_by_actor.values())) <= 1 else 0
        window_summary = window_count_explanation(len(rows), len(manifest["senders"]), windows_per_machine)
        predicted_attack = sum(int(row["prediction"]) == 1 for row in latest.values())
        checked_count = sum(outcomes.values())
        provisional = len(rows) - checked_count
        received = sum(int(row.get("sent", 0)) for row in manifest["senders"])
        stream_fresh = progress.get("status") == "running" and time.time() - float(progress.get("heartbeat", 0)) < 10
        stream_label, stream_message = stream_feedback(
            manifest.get("status"), stream_fresh, checked_count, len(rows)
        )
        if manifest.get("status") == "completed":
            if rows and checked_count == len(rows):
                st.success(
                    f"Hoàn tất: Spark đã đối chiếu {checked_count} lượt máy–10 giây — "
                    f"bắt đúng {outcomes.get('Bắt đúng', 0)}, bỏ sót {outcomes.get('Bỏ sót', 0)}, "
                    f"báo nhầm {outcomes.get('Báo nhầm', 0)}."
                )
            elif rows:
                st.info(f"15 máy đã gửi xong. Spark đang chốt kết quả: {checked_count}/{len(rows)} khoảng 10 giây.")
            else:
                st.info("15 máy đã gửi xong. Spark chưa ghi nhận khoảng dữ liệu nào cho lượt này.")
        first, second, third, fourth = st.columns(4)
        if manifest.get("status") == "completed":
            first.metric("Máy gửi xong", f"{sum(row['status'] == 'Hoàn tất' for row in manifest['senders'])}/15")
        elif manifest.get("status") in {"running", "preparing"}:
            first.metric("Máy đang gửi", f"{sum(row['status'] == 'Đang gửi' for row in manifest['senders'])}/15")
        else:
            first.metric("Máy đã gửi dữ liệu", f"{sum(int(row.get('sent', 0)) > 0 for row in manifest['senders'])}/15")
        second.metric("Kết nối máy chủ nhận", f"{received:,}")
        third.metric("Máy Spark nghi ngờ · 10 giây gần nhất", predicted_attack)
        fourth.metric("Lượt máy–10 giây đã đối chiếu", f"{checked_count}/{len(rows)}")
        st.caption(f"Mã lượt {experiment_id} · Spark {stream_label}. Mỗi khoảng tương ứng một máy trong 10 giây.")
        st.caption(window_summary)

        st.markdown("#### Spark xử lý được bao nhiêu flow mỗi giây?")
        batch_history = list(progress.get("recent_batches", []))
        if not batch_history:
            history_key = f"spark_throughput_{experiment_id}"
            batch_history = st.session_state.setdefault(history_key, [])
            remember_spark_batch(batch_history, {
                "batch_id": progress.get("stream_batch_id", progress.get("batch_id", 0)),
                "input_rows": progress.get("input_rows", 0),
                "batch_seconds": progress.get("batch_seconds", 0),
            })
        measured_batches = spark_throughput_rows(batch_history)
        if measured_batches:
            latest_batch = measured_batches[-1]
            rate_col, chart_col = st.columns([1, 3])
            with rate_col:
                st.markdown(metric_cards_html([
                    ("Lô gần nhất có dữ liệu", f"{latest_batch['Flow/giây']:,.0f} flow/s", "neutral")
                ], columns=1), unsafe_allow_html=True)
            with chart_col:
                st.line_chart(
                    pd.DataFrame(measured_batches),
                    x="Lô Spark",
                    y="Flow/giây",
                    x_label="Các lô dữ liệu Spark xử lý",
                    y_label="Flow mỗi giây",
                    height=250,
                )
            st.caption(
                f"Lô {latest_batch['Lô Spark']}: {latest_batch['Flow trong lô']:,} flow ÷ "
                f"{latest_batch['Thời gian xử lý (giây)']:.2f} giây. Flow ở đây là một kết nối đã ghi nhận."
            )
        else:
            st.caption("Chưa có lô chứa flow để đo tốc độ; lô rỗng không được tính là 0 flow/giây.")

        if progress.get("error"):
            st.error(f"Spark stream: {progress['error']}")
        elif not stream_fresh:
            st.info(stream_message)

        display = []
        for index, sender in enumerate(manifest["senders"]):
            actor = sender["actor_id"]
            result = latest.get(actor)
            truth_result = checked.get(actor, {})
            profile = sender.get("profile", "normal")
            if result:
                is_final = truth_result.get("checked", False)
                outcome = truth_result.get("outcome", "Chờ") if is_final else "Chờ"
                outcome = {
                    "Bình thường đúng": "Nhận đúng",
                    "Chờ đủ dữ liệu đối chiếu": "Chờ",
                }.get(outcome, outcome)
                decision = "NGHI NGỜ" if result["prediction"] else "BÌNH THƯỜNG"
                display.append({"Máy": names[actor], "Hành vi": {
                    "Truy cập thường": "Thường", "Truy cập nhanh hợp lệ": "Web nhanh",
                    "Quét nhiều cổng": "Quét cổng", "Quét cổng chậm": "Quét chậm",
                    "Gửi dồn": "Gửi dồn", "Gửi dồn ngắt quãng": "Gửi ngắt quãng",
                }.get(LIVE_NAMES.get(profile, profile), profile),
                    "Đã gửi": sender.get("sent", 0), "Flow / 10s": int(result["flow_count"]),
                    "Flow/s": round(float(result["flow_rate"]), 2),
                    "Cổng mồi": f"{float(result['port_scan_ratio']):.0%}",
                    "MLlib kết luận": decision, "Điểm nghi ngờ": round(float(result["attack_probability"]), 3),
                    "Đối chiếu": outcome})
            else:
                display.append({"Máy": names[actor], "Hành vi": LIVE_NAMES.get(profile, profile),
                    "Đã gửi": sender.get("sent", 0), "Flow / 10s": 0, "Flow/s": 0,
                    "Cổng mồi": "—", "MLlib kết luận": "Chờ Spark", "Điểm nghi ngờ": None,
                    "Đối chiếu": "Chờ"})
        st.markdown("#### Mỗi máy đang được Spark đánh giá thế nào?")
        st.caption("Mỗi hàng = 1 máy trong 10 giây gần nhất. Flow/10s = số kết nối; Flow/s = tốc độ trung bình; Cổng mồi = tỷ lệ quét; Điểm = mức nghi ngờ MLlib.")
        st.dataframe(pd.DataFrame(display), hide_index=True, width="stretch", height=410, column_config={
            "Máy": st.column_config.TextColumn("Máy", width=55),
            "Hành vi": st.column_config.TextColumn("Hành vi", width=125),
            "Đã gửi": st.column_config.NumberColumn("Đã gửi", width=55),
            "Flow / 10s": st.column_config.NumberColumn("Flow/10s", width=72, format="%d"),
            "Flow/s": st.column_config.NumberColumn("Flow/s", width=60, format="%.2f"),
            "Cổng mồi": st.column_config.TextColumn("Cổng mồi", width=65),
            "MLlib kết luận": st.column_config.TextColumn("MLlib", width=88),
            "Điểm nghi ngờ": st.column_config.NumberColumn("Điểm", width=60, format="%.3f"),
            "Đối chiếu": st.column_config.TextColumn("Đối chiếu", width=90),
        })

        st.markdown("#### Kết quả phát hiện")
        st.markdown(metric_cards_html([
            ("Bắt đúng bất thường", f"{outcomes.get('Bắt đúng', 0):,}", "good"),
            ("Bỏ sót bất thường", f"{outcomes.get('Bỏ sót', 0):,}", "bad"),
            ("Báo nhầm", f"{outcomes.get('Báo nhầm', 0):,}", "warning"),
            ("Bình thường nhận đúng", f"{outcomes.get('Bình thường đúng', 0):,}", "neutral"),
        ], columns=4), unsafe_allow_html=True)
        if provisional:
            st.caption(f"{provisional} khoảng còn chờ Spark chốt dữ liệu.")

        with st.expander("Biểu đồ: lưu lượng một máy và điểm MLlib", expanded=False):
            selected = st.session_state.get("live_actor", SENDER_IDS[0])
            series = sorted([row for row in rows if row.get("actor_id") == selected], key=lambda row: row["window_start"])
            if series:
                frame = pd.DataFrame([
                    {
                        "Cửa sổ": f"#{index + 1}",
                        "Kết nối/giây (trung bình)": float(row["flow_rate"]),
                        "Điểm nghi ngờ MLlib": float(row["attack_probability"]),
                    }
                    for index, row in enumerate(series)
                ])
                st.markdown(f"**{names.get(selected, 'Máy đã chọn')} · mỗi điểm = một khoảng 10 giây**")
                st.caption("Traffic là các kết nối được máy gửi đến server. Tốc độ = số kết nối trong 10 giây chia 10.")
                graph_left, graph_right = st.columns(2)
                graph_left.markdown("**Máy gửi bao nhiêu kết nối?**")
                graph_left.line_chart(
                    frame, x="Cửa sổ", y="Kết nối/giây (trung bình)",
                    x_label="Mốc 10 giây", y_label="Kết nối/giây", height=260,
                )
                graph_right.markdown("**MLlib nghi ngờ đến mức nào?**")
                score_frame = frame[["Cửa sổ", "Điểm nghi ngờ MLlib"]].copy()
                threshold = current_metrics.get("threshold")
                if threshold is not None:
                    score_frame["Ngưỡng cảnh báo"] = float(threshold)
                graph_right.line_chart(
                    score_frame, x="Cửa sổ", x_label="Mốc 10 giây", y_label="Điểm (0–1)", height=260,
                )
                if threshold is not None:
                    st.caption(f"Điểm MLlib càng gần 1 càng đáng ngờ; cảnh báo khi đạt ngưỡng {float(threshold):.2f}.")
            else:
                st.caption("Chưa có cửa sổ 10 giây hoàn tất.")
    else:
        score = current_metrics.get("test", {})
        if score:
            baseline = current_metrics.get("rule_test", {})
            a, b, c = st.columns(3)
            a.metric("Kiểm tra chưa thấy", f"{score.get('total', 0)} cửa sổ")
            b.metric("Random Forest · F1", f"{score.get('f1', 0):.0%}")
            c.metric("Luật đơn giản · F1", f"{baseline.get('f1', 0):.0%}")
            st.caption("Train, chọn ngưỡng và kiểm tra dùng ba lần thu Docker độc lập. Dữ liệu mô phỏng lab, không đại diện Internet.")

    with st.expander("Spark làm gì trong demo này?", expanded=False):
        st.markdown(
            "**Máy chủ Docker** ghi nhận kết nối thật → **Spark Structured Streaming** gom kết nối "
            "theo từng máy trong khoảng 10 giây → **Spark MLlib Random Forest** chấm điểm bất thường từ 0 đến 1."
        )
        st.caption("Spark chạy local[2] trên máy này. Nhãn để đối chiếu được lưu riêng, mô hình không dùng nhãn đó khi dự đoán.")


def read_windows(current_experiment: str) -> list[dict]:
    snapshot = read_json(ROOT / "outputs/live_lab/windows/windows.json", {"rows": []})
    return [row for row in snapshot.get("rows", []) if row.get("experiment_id") == current_experiment]


def latest_by_actor(rows: list[dict]) -> dict[str, dict]:
    latest = {}
    for row in sorted(rows, key=lambda item: item.get("window_start", "")):
        latest[row.get("actor_id")] = row
    return {actor: row for actor, row in latest.items() if actor}


render_console()

with st.expander("Bài đo dữ liệu lớn · UNSW-NB15", expanded=False):
    st.write("Đo Spark xử lý được bao nhiêu flow trong một giây khi dữ liệu tăng. Cùng một phép tính, đổi quy mô dữ liệu và số partition.")
    scale = read_json(ROOT / "outputs/live_lab/scale.json")
    if scale.get("status") in {"downloading", "bronze", "silver", "benchmarking"}:
        st.info(f"Đang chuẩn bị và đo dữ liệu: {scale['status']}.")
    elif scale.get("status") == "failed":
        st.error(scale.get("error", "Phép đo chưa hoàn thành."))
    result_path = ROOT / "outputs/benchmarks/live_lab_scale.json"
    result = read_json(result_path)
    if result.get("status") == "ok":
        st.caption(f"Dữ liệu thật: {result['total_train_rows']:,} flow train · {result['input_parquet_bytes'] / 1_000_000:.1f} MB Parquet · Spark {result['spark_version']} · {result['local_master']} trên laptop.")
        st.caption("Cột cao hơn = Spark tổng hợp được nhiều flow mỗi giây. Đây là tốc độ phép đo trên máy này, không phải cluster.")
        points = []
        for group in result["windows"]:
            for partition in group["partition_results"]:
                median_seconds = float(partition["query_timing"]["median_seconds"])
                points.append({"Quy mô dữ liệu": f"{group['input_rows']:,}", "Flow đầu vào": group["input_rows"],
                    "Partition": str(partition["actual_partitions"]),
                    "Tốc độ Spark (flow/giây)": round(group["input_rows"] / median_seconds, 1) if median_seconds > 0 else None,
                    "Trung vị xử lý (giây)": median_seconds,
                    "Task hoàn tất": partition["task_count"]})
        frame = pd.DataFrame(points)
        st.markdown("#### Tốc độ xử lý theo quy mô dữ liệu")
        graph, ledger = st.columns([3, 2])
        with graph:
            render_scale_chart(st, frame)
        with ledger:
            st.dataframe(frame, hide_index=True, width="stretch")
        st.caption("Flow/giây = số flow đầu vào ÷ trung vị thời gian của 3 lần đo. Dữ liệu đã cache; không tính thời gian tải/làm sạch. Tốc độ còn phụ thuộc RAM và CPU.")
    elif scale.get("status") not in {"downloading", "bronze", "silver", "benchmarking"}:
        st.info("Chưa có bộ UNSW-NB15 trong môi trường này. Nút này tải CSV có checksum, tạo Bronze/Silver Parquet rồi đo trên dữ liệu thật.")
    if st.button("Tải dữ liệu đã kiểm chứng và chạy phép đo", disabled=scale.get("status") in {"downloading", "bronze", "silver", "benchmarking"}):
        try:
            controller.start_scale()
            st.rerun()
        except Exception as error:
            st.error(str(error))


"""Main, result-first page for full-data UNSW-NB15 Spark IDS."""

from __future__ import annotations

import html
import json
import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.full_unsw_access import default_full_data_root, load_full_unsw_status
from dashboard.full_unsw_controller import FullUNSWController
from dashboard.styles import inject_styles
from dashboard.ui import metric_cards_html


st.set_page_config(page_title="UNSW-NB15 · Spark IDS", page_icon="⚡", layout="wide")
inject_styles()
st.markdown(
    """
    <style>
      .block-container {max-width:1180px; padding-top:1.25rem}
      .full-hero {border-top:7px solid #14201e;border-bottom:1px solid #14201e;padding:1rem 0 1.15rem;margin-bottom:1rem}
      .full-kicker {color:#ed5b24;font:800 .72rem 'Bahnschrift',sans-serif;letter-spacing:.17em}
      .full-hero h1 {font-size:clamp(2rem,4.3vw,3.65rem);line-height:1.02;max-width:900px;margin:.55rem 0}
      .full-hero p {max-width:850px;color:#53615c;font-size:1rem;margin:0}
      .full-flow {display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:.55rem;margin:1rem 0 1.2rem}
      .full-flow div {background:#fbfaf6;border:1px solid #14201e;border-top:4px solid #ed5b24;padding:.7rem .8rem}
      .full-flow b,.full-flow span {display:block}
      .full-flow b {font-size:.9rem}
      .full-flow span {color:#66716d;font-size:.76rem;margin-top:.2rem}
      .full-run {padding:.8rem 1rem;border-left:4px solid #17624b;background:#d9ebe2;margin:.5rem 0 1rem}
      .full-run.pending {border-left-color:#ed5b24;background:#fbe0d2}
      .full-run strong {display:block;margin-bottom:.18rem}
      .full-run span {font-size:.86rem;color:#53615c}
      .full-section {margin-top:1.5rem;border-top:1px solid #14201e;padding-top:.6rem}
      @media(max-width:700px){.full-flow{grid-template-columns:repeat(2,minmax(0,1fr))}}
      @media(max-width:440px){.full-flow{grid-template-columns:1fr}}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def controller_for(root: str) -> FullUNSWController:
    return FullUNSWController(root)


def _fmt_count(value) -> str:
    return f"{int(value):,}" if value is not None else "—"


def _render_results(run: dict) -> None:
    source = run.get("source_manifest", {})
    quality = run.get("quality_before_cleaning", {})
    splits = run.get("splits", {})
    lock = run.get("model_lock", {})
    test = run.get("test", {})
    metrics = test.get("metrics", {})

    st.markdown('<div class="full-section"><h2>Kết quả trên test độc lập</h2></div>', unsafe_allow_html=True)
    st.caption("Model và ngưỡng được chốt trên train/validation trước khi chấm tập test.")
    st.markdown(metric_cards_html([
        ("Flow test đã chấm", _fmt_count(test.get("prediction_rows")), "neutral"),
        ("Bắt đúng tấn công", _fmt_count(metrics.get("true_positive")), "good"),
        ("Bỏ sót tấn công", _fmt_count(metrics.get("false_negative")), "bad"),
        ("Báo nhầm", _fmt_count(metrics.get("false_positive")), "warning"),
    ]), unsafe_allow_html=True)

    st.markdown(metric_cards_html([
        ("Precision", f"{100 * metrics.get('precision', 0):.1f}%", "good"),
        ("Recall", f"{100 * metrics.get('recall', 0):.1f}%", "good"),
        ("F1", f"{100 * metrics.get('f1', 0):.1f}%", "good"),
        ("Báo động giả", f"{100 * metrics.get('false_positive_rate', 0):.1f}%", "warning"),
    ]), unsafe_allow_html=True)

    st.markdown("**Bảng đối chiếu: dự đoán của model so với nhãn thật**")
    st.dataframe(
        [
            {"Nhãn thật": "Tấn công", "Báo là tấn công": _fmt_count(metrics.get("true_positive")), "Báo bình thường": _fmt_count(metrics.get("false_negative"))},
            {"Nhãn thật": "Bình thường", "Báo là tấn công": _fmt_count(metrics.get("false_positive")), "Báo bình thường": _fmt_count(metrics.get("true_negative"))},
        ],
        hide_index=True,
        width="stretch",
    )
    examples = test.get("examples", [])
    if examples:
        st.markdown("**Một vài flow để kiểm tra bằng mắt**")
        st.dataframe(
            [
                {
                    "Kết quả": item.get("outcome"),
                    "Loại flow": item.get("attack_cat"),
                    "Giao thức": item.get("proto"),
                    "Dịch vụ": item.get("service"),
                    "Điểm tấn công": round(float(item.get("attack_score", 0)), 3),
                    "File nguồn": item.get("source_file"),
                }
                for item in examples
            ],
            hide_index=True,
            width="stretch",
        )

    with st.expander("Dữ liệu, cách chia và bằng chứng Spark"):
        st.markdown(
            f"**Nguồn:** {source.get('file_count', '—')} CSV · {_fmt_count(quality.get('rows'))} dòng gốc · "
            f"{source.get('total_bytes', 0) / (1024**2):.1f} MB.  **Model:** {lock.get('model', '—')} · "
            f"fit {_fmt_count(lock.get('fit_rows'))} flow · threshold {lock.get('decision_threshold', '—')}."
        )
        published_rows = run.get("published_source_rows")
        row_difference = run.get("source_vs_published_row_difference")
        if published_rows is not None and row_difference:
            st.info(
                f"Đối chiếu nguồn: UNSW ghi {published_rows:,} dòng; bốn CSV đã xác minh bằng checksum có "
                f"{quality.get('rows', 0):,} dòng ({row_difference:+,}). Giữ nguyên toàn bộ dòng trong CSV, không tự xóa phần chênh."
            )
        st.dataframe(
            [
                {"Tập dữ liệu": name, "Số flow": _fmt_count(value.get("rows")), "Nhãn": value.get("label_counts", {})}
                for name, value in splits.items()
            ],
            hide_index=True,
            width="stretch",
        )
        quality_after = run.get("quality_after_cleaning", {})
        st.write(
            f"Dòng trùng được giữ nhưng cùng split: {_fmt_count(quality.get('duplicate_rows'))} · "
            f"Nhóm nhãn mâu thuẫn bị loại: {_fmt_count(quality.get('conflicting_fingerprint_groups'))} · "
            f"Ô số không đọc được: {_fmt_count(sum(quality.get('malformed_numeric_counts', {}).values()))} · "
            f"Ô số bị trống: {_fmt_count(sum(quality.get('missing_numeric_counts', {}).values()))}."
        )
        st.caption(
            f"Spark {run.get('spark_version', '—')} · {run.get('spark_master', '—')} · "
            f"chuẩn bị {run.get('prepare_seconds', 0):.1f}s · fit {lock.get('fit_seconds', 0):.1f}s · "
            f"chấm test {test.get('score_seconds', 0):.1f}s. Job/Stage/Task là số Spark ghi nhận trong lượt này."
        )
        evidence_rows = []
        for label, evidence in (
            ("Đọc/làm sạch/chia dữ liệu", run.get("spark_evidence", {})),
            ("Chấm tập test", test.get("spark_evidence", {})),
        ):
            if evidence:
                evidence_rows.append(
                    {
                        "Spark đã làm": label,
                        "Job": _fmt_count(evidence.get("job_count")),
                        "Stage ID": _fmt_count(evidence.get("stage_count")),
                        "Task tạo": _fmt_count(evidence.get("tasks_created")),
                        "Task hoàn tất": _fmt_count(evidence.get("tasks_completed")),
                        "Task lỗi": _fmt_count(evidence.get("tasks_failed")),
                    }
                )
        if evidence_rows:
            st.markdown("**Dấu vết thực thi Spark trong lượt này**")
            st.dataframe(evidence_rows, hide_index=True, width="stretch")
            st.caption(
                "Spark có thể tạo stage cho kế hoạch tối ưu rồi bỏ qua stage không cần chạy; "
                "vì vậy Task tạo có thể nhiều hơn Task hoàn tất. Task lỗi là số Spark ghi nhận, không phải số bị bỏ qua."
            )
        st.caption(
            f"Mã lượt: {run.get('run_id', '—')} · "
            f"{test.get('evaluation_protocol', 'Test độc lập, model đã khóa trước khi chấm.')}"
        )
        st.dataframe(
            [
                {"Tệp": item.get("name"), "Kích thước (byte)": item.get("bytes"), "SHA-256": item.get("sha256")}
                for item in source.get("files", [])
            ],
            hide_index=True,
            width="stretch",
        )


st.markdown(
    '<div class="full-hero"><div class="full-kicker">ĐỒ ÁN · PHÁT HIỆN XÂM NHẬP BẰNG APACHE SPARK</div>'
    '<h1>2.54 triệu flow mạng đi qua Spark MLlib như thế nào?</h1>'
    '<p>UNSW-NB15 cung cấp các lượt kết nối mạng đã gắn nhãn. Spark chuẩn hóa và chia dữ liệu; MLlib học cách phân biệt bình thường với tấn công; tập test riêng cho biết model đúng và sai ở đâu.</p></div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="full-flow"><div><b>1 · Dữ liệu</b><span>4 CSV UNSW-NB15</span></div>'
    '<div><b>2 · Apache Spark</b><span>Đọc, làm sạch, chia tập bằng Parquet</span></div>'
    '<div><b>3 · Spark MLlib</b><span>Logistic Regression học trên train</span></div>'
    '<div><b>4 · Kết quả</b><span>Test riêng: bắt đúng, báo nhầm, bỏ sót</span></div></div>',
    unsafe_allow_html=True,
)

data_root = default_full_data_root()
controller = controller_for(str(data_root))
try:
    initial_state = load_full_unsw_status(data_root)["status"]
except (OSError, ValueError, json.JSONDecodeError):
    initial_state = "invalid"
refresh_pipeline = initial_state in {"starting", "preparing", "training", "prepared"} and controller.active


@st.fragment(run_every="2s" if refresh_pipeline else None)
def render_pipeline_status():
    try:
        status_view = load_full_unsw_status(data_root)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        st.error(f"Không đọc được trạng thái lượt chạy: {error}")
        status_view = {"status": "invalid", "ready": False, "run": {}, "root": str(data_root)}
    run = status_view["run"]
    state = status_view["status"]
    labels = {
        "missing": ("Chưa có kết quả", "Dữ liệu chưa được chạy qua pipeline Spark."),
        "starting": ("Đang khởi động Spark", "Đang tạo trạng thái lượt chạy."),
        "preparing": ("Spark đang chuẩn bị dữ liệu", "Đọc 4 CSV, kiểm tra chất lượng, ghi Parquet và chia tập."),
        "prepared": ("Dữ liệu đã sẵn sàng", "Bước tiếp theo là huấn luyện MLlib và chấm test độc lập."),
        "training": ("MLlib đang huấn luyện / chấm test", "Model học trên train; test được giữ riêng đến bước cuối."),
        "succeeded": ("Đã hoàn thành", "Kết quả dưới đây lấy từ đúng lượt Spark này."),
        "failed": ("Lượt chạy bị lỗi", str(run.get("error", "Xem log để biết bước gặp lỗi."))),
        "invalid": ("Không đọc được trạng thái", "Kiểm tra file latest.json và run.json."),
    }
    title, explanation = labels.get(state, ("Trạng thái không xác định", str(state)))
    st.markdown(
        f'<div class="full-run {"" if state == "succeeded" else "pending"}"><strong>{html.escape(title)}</strong>'
        f'<span>{html.escape(explanation)}</span></div>',
        unsafe_allow_html=True,
    )

    progress = run.get("progress", {})
    total_steps = max(1, int(progress.get("total_steps", 8)))
    completed_steps = min(total_steps, max(0, int(progress.get("completed_steps", total_steps if state == "succeeded" else 0))))
    current_step = progress.get("current_step") or ("Pipeline đã hoàn thành" if state == "succeeded" else "Đang chờ bước Spark cập nhật")
    if state in {"starting", "preparing", "prepared", "training", "succeeded", "failed"}:
        st.progress(
            completed_steps / total_steps,
            text=f"{completed_steps}/{total_steps} bước xong · {current_step}",
        )
        st.caption(f"Tiến trình: {completed_steps}/{total_steps} bước · Đang làm: {current_step}. Thanh dựa trên các mốc Spark đã ghi nhận, không ước lượng thời gian.")

    if state in {"preparing", "training", "starting"} and not controller.active:
        st.info("Trạng thái cho biết Spark đang chạy; nếu tiến trình bị đóng, xem log trong data/full_unsw/logs.")
    else:
        action_label = "Tiếp tục huấn luyện MLlib" if state == "prepared" else "Chạy pipeline UNSW đầy đủ"
        if st.button(action_label, type="primary", disabled=state in {"preparing", "training", "starting", "invalid"}):
            try:
                log_path = controller.start(current_status=state, run_id=status_view.get("run_id"))
                st.success("Đã khởi chạy Spark ở chế độ nền; tiến trình sẽ tự cập nhật.")
                st.caption(f"Log chi tiết: {log_path}")
                st.rerun()
            except Exception as error:
                st.error(str(error))

    if state == "succeeded":
        _render_results(run)
    elif state == "failed":
        st.caption(f"Mã lượt chạy: {run.get('run_id', '—')} · Log: {data_root / 'logs'}")
    elif state == "missing":
        st.caption("Lần đầu chạy có thể mất vài phút. Dữ liệu, Parquet, model và kết quả đều đặt trên ổ D; model Docker được giữ riêng. UNSW ghi 2,540,044 dòng; bản CSV đã xác minh có 2,540,047 dòng, giữ nguyên cả 3 dòng chênh.")

    still_running = state in {"starting", "preparing", "training"} or (state == "prepared" and controller.active)
    if refresh_pipeline and not still_running:
        st.rerun()


render_pipeline_status()

st.markdown('<div class="full-section"><h2>Demo Docker là phần khác</h2></div>', unsafe_allow_html=True)
st.caption("UNSW-NB15 chứng minh học và đánh giá trên bộ dữ liệu lớn. Docker Live Lab mô phỏng máy gửi traffic theo thời gian thực; đó là nhánh minh họa streaming, không dùng chung model với UNSW.")
left, right = st.columns(2)
with left:
    st.page_link("pages/01_Live_Lab.py", label="Mở Docker Live Lab · 15 máy gửi")
with right:
    st.page_link("pages/02_Spark_Guided_Demo.py", label="Mở giải thích Job / Stage / Task")

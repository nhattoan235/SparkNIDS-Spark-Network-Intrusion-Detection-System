"""Step-by-step explanation of Spark concepts used in the project."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.data_access import load_demo_status
from dashboard.guided_demo import render_guided_demo
from dashboard.styles import inject_styles


st.set_page_config(page_title="Giải thích Spark · Spark IDS", page_icon="⚡", layout="wide")
inject_styles()


@st.cache_data(ttl=1, show_spinner=False)
def current_status():
    return load_demo_status()


status = current_status()
status_label = {
    "running": "Đang chạy",
    "succeeded": "Đã hoàn thành",
    "failed": "Có bước lỗi",
    "not_ready": "Đang chuẩn bị",
}.get(status.get("status"), str(status.get("status", "—")))
st.markdown(
    f'<div class="lab-hero"><div class="lab-brand">SPARK IDS LAB <span>GIẢI THÍCH THEO TỪNG BƯỚC</span></div>'
    f'<div class="lab-hero-row"><div><h1>Một flow mạng đi qua Spark như thế nào?</h1>'
    f'<p class="hero-explainer">Flow là một bản ghi tóm tắt một lượt giao tiếp mạng.</p></div>'
    f'<div class="run-stamp"><small>TRẠNG THÁI</small><strong>{status_label}</strong>'
    f'<span>{status.get("run_id") or "Chưa có run"}</span></div></div></div>',
    unsafe_allow_html=True,
)
if status.get("fallback"):
    st.info("Đang dùng dữ liệu của lần chạy gần nhất.")
elif status.get("status") == "not_ready":
    st.warning("Chưa có dữ liệu chạy demo. Đây là trang giải thích khái niệm, không phải kết quả UNSW full-data.")

render_guided_demo(status)

"""Offline-first guided presentation dashboard for the Spark IDS project."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.data_access import load_demo_status
from dashboard.guided_demo import render_guided_demo
from dashboard.styles import inject_styles


st.set_page_config(page_title="Spark IDS · Guided Demo", page_icon="⚡", layout="wide")
inject_styles()


@st.cache_data(ttl=1, show_spinner=False)
def current_status():
    return load_demo_status()


status = current_status()
run_label = "Artifact offline" if status.get("fallback") else status.get("run_id") or "Chưa có run"
status_label = {
    "running": "Đang chạy",
    "succeeded": "Đã hoàn thành",
    "failed": "Có step lỗi",
    "not_ready": "Đang chuẩn bị",
}.get(status.get("status"), str(status.get("status", "—")))
st.markdown(
    f'<div class="lab-hero"><div class="lab-brand">SPARK IDS LAB <span>UNSW-NB15</span></div>'
    f'<div class="lab-hero-row"><div><h1>Một flow mạng đi qua Spark như thế nào?</h1>'
    f'<p class="hero-explainer">Flow là một bản ghi tóm tắt một lượt giao tiếp mạng.</p></div>'
    f'<div class="run-stamp"><small>TRẠNG THÁI</small><strong>{status_label}</strong><span>{run_label}</span></div></div></div>',
    unsafe_allow_html=True,
)
if status.get("fallback"):
    st.info("Đang dùng dữ liệu của lần chạy gần nhất.")
elif status.get("status") == "running":
    st.caption("Spark đang cập nhật số liệu.")
elif status.get("status") == "not_ready":
    st.warning("Chưa có dữ liệu chạy demo.")

render_guided_demo(status)

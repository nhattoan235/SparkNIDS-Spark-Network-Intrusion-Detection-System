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
    f'<div class="guided-hero"><div><div class="guided-kicker">Apache Spark · UNSW-NB15 · IDS</div>'
    f'<h1>Nhìn thấy Spark đang làm gì</h1><p>Đi qua một network flow từ dữ liệu vào đến cảnh báo Attack.</p></div>'
    f'<div class="guided-status"><strong>{status_label}</strong>{run_label}<br><span>7 bước trình bày</span></div></div>',
    unsafe_allow_html=True,
)
if status.get("fallback"):
    st.info("Đang hiển thị artifact offline đã export. Chạy runner để xem bằng chứng Spark live và mở Spark UI.")
elif status.get("status") == "running":
    st.caption("Runner đang cập nhật artifact. Tải lại trang sau mỗi step để xem số liệu mới.")
elif status.get("status") == "not_ready":
    st.warning("Runner chưa tạo status hoàn chỉnh; đang chờ artifact hợp lệ.")

render_guided_demo(status)

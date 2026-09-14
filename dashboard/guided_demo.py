"""Presentation components for the seven-step Spark story."""

from __future__ import annotations

import html
from typing import Any

import pandas as pd
import streamlit as st


def _step_title(step: dict[str, Any]) -> str:
    return str(step.get("title") or step.get("id", "Spark step"))


def render_progress(steps: list[dict[str, Any]], current_index: int) -> None:
    nodes = []
    for index, step in enumerate(steps):
        step_status = str(step.get("status", "pending"))
        classes = "step-node active" if index == current_index else "step-node done" if step_status == "succeeded" else "step-node"
        nodes.append(f'<div class="{classes}"><span class="number">{index + 1:02d}</span>{html.escape(_step_title(step))}</div>')
    st.markdown(f'<div class="stepper">{"".join(nodes)}</div>', unsafe_allow_html=True)


def render_flow(flow: dict[str, Any]) -> None:
    cards = (("Input", flow.get("input", "Dữ liệu vào"), ""), ("Spark xử lý", flow.get("spark", "DataFrame / Spark SQL"), "spark"), ("Kết quả", flow.get("result", "Kết quả"), "result"))
    rendered = []
    for index, (label, value, extra_class) in enumerate(cards):
        rendered.append(f'<div class="flow-card {extra_class}"><small>{html.escape(label)}</small><strong>{html.escape(str(value))}</strong></div>')
        if index < 2:
            rendered.append('<div class="flow-arrow">→</div>')
    st.markdown(f'<div class="flow-wrap"><div class="flow-label">Một dòng để nhớ</div><div class="flow">{"".join(rendered)}</div></div>', unsafe_allow_html=True)


def render_metrics(metrics: list[dict[str, Any]]) -> None:
    visible = metrics[:3]
    if not visible:
        return
    columns = st.columns(len(visible))
    for column, metric in zip(columns, visible, strict=True):
        column.metric(str(metric.get("label", "Metric")), str(metric.get("value", "—")))


def render_spark_evidence(step: dict[str, Any]) -> None:
    evidence = step.get("evidence", {}) or {}
    result = step.get("result", {}) or {}
    if not evidence and not result:
        return
    st.markdown('<div class="evidence-title">Bằng chứng Spark</div>', unsafe_allow_html=True)
    tracker = evidence.get("status_tracker") or result.get("status_tracker")
    if tracker:
        st.code("Job: {job_count}  |  Stage: {stage_count}  |  Task khai báo: {tasks}  |  Task lỗi: {failed}".format(job_count=tracker.get("job_count", 0), stage_count=tracker.get("unique_stage_count", 0), tasks=tracker.get("total_tasks_across_unique_stages", 0), failed=tracker.get("failed_tasks_across_unique_stages", 0)), language="text")
    if evidence.get("shuffle_exchange_nodes") or result.get("shuffle_exchange_nodes"):
        exchange = evidence.get("shuffle_exchange_nodes", result.get("shuffle_exchange_nodes"))
        st.caption(f"Physical plan có {exchange} Exchange — dấu hiệu shuffle khi groupBy/orderBy.")
    if evidence.get("pipeline_stage_names") or result.get("pipeline_stage_names"):
        stages = evidence.get("pipeline_stage_names", result.get("pipeline_stage_names"))
        st.code(" → ".join(str(stage) for stage in stages), language="text")
    plan = evidence.get("physical_plan") or result.get("physical_plan")
    if plan:
        with st.expander("Xem physical plan rút gọn"):
            st.code(str(plan)[-3500:], language="text")
    if evidence.get("restart_checks"):
        checks = evidence["restart_checks"]
        if all(bool(value) for value in checks.values()):
            st.success("Checkpoint/restart hợp lệ: lượt sau chỉ nhận file mới.")
        else:
            st.warning("Một số kiểm tra checkpoint chưa đạt.")
        with st.expander("Xem kiểm tra restart"):
            st.json(checks)
    if evidence.get("schema"):
        schema = evidence["schema"]
        st.caption(f"Schema Spark: {len(schema)} cột, hiển thị mẫu 8 cột đầu.")
        st.dataframe(pd.DataFrame(schema).head(8), width="stretch", hide_index=True)
    if evidence.get("source"):
        st.caption(f"Nguồn offline: {evidence['source']}")


def render_step(step: dict[str, Any]) -> None:
    index = int(step.get("index", 1))
    st.subheader(f"Bước {index} · {_step_title(step)}")
    status = str(step.get("status", "pending"))
    if status == "failed":
        error = step.get("error", {})
        st.error(f"Step chưa chạy được: {error.get('message', 'Lỗi không xác định')}")
        if error.get("recovery_hint"):
            st.info(f"Cách xử lý: {error['recovery_hint']}")
    elif status == "pending":
        st.info("Step này chưa được runner thực thi. Có thể chạy riêng bằng --step.")
    elif status == "running":
        st.warning("Spark đang xử lý step này; tải lại trang để nhận trạng thái mới nhất.")
    elif status == "succeeded":
        st.caption("Đã hoàn thành · số liệu dưới đây được ghi bởi runner Spark.")
    render_flow(step.get("flow", {}))
    render_metrics(step.get("metrics", []))
    if step.get("explanation"):
        st.markdown(f"**Ý nghĩa:** {step['explanation']}")
    render_spark_evidence(step)


def render_navigation(current_index: int, total_steps: int, spark_ui_url: str | None) -> None:
    back, spark_col, next_col = st.columns([1, 1.2, 1])
    with back:
        if st.button("Quay lại", disabled=current_index <= 0, use_container_width=True):
            st.session_state.demo_step = max(0, current_index - 1)
            st.rerun()
    with spark_col:
        if spark_ui_url:
            st.link_button("Mở Spark UI", spark_ui_url, use_container_width=True)
        else:
            st.button("Spark UI chưa mở", disabled=True, use_container_width=True)
    with next_col:
        if st.button("Tiếp theo", type="primary", disabled=current_index >= total_steps - 1, use_container_width=True):
            st.session_state.demo_step = min(total_steps - 1, current_index + 1)
            st.rerun()


def render_guided_demo(status: dict[str, Any]) -> None:
    steps = status.get("steps", [])
    if not steps:
        st.error("Không có step nào trong demo artifact.")
        return
    current_index = int(st.session_state.get("demo_step", 0))
    current_index = min(max(current_index, 0), len(steps) - 1)
    st.session_state.demo_step = current_index
    render_progress(steps, current_index)
    render_step(steps[current_index])
    render_navigation(current_index, len(steps), status.get("spark_ui_url"))

"""Table-first presentation components for the seven-step Spark story."""

from __future__ import annotations

import html
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.presentation import STAGES, build_step_view, stage_for_step


def _step_title(step: dict[str, Any]) -> str:
    return str(step.get("title") or step.get("id", "Spark step"))


def render_stage_rail(step_id: str) -> None:
    current = stage_for_step(step_id)
    current_number = str(current["number"])
    nodes = []
    for stage in STAGES:
        number = str(stage["number"])
        state = "active" if number == current_number else "done" if number < current_number else ""
        nodes.append(
            f'<div class="lab-stage {state}"><span>{number}</span>'
            f'<strong>{html.escape(str(stage["title"]))}</strong></div>'
        )
    st.markdown(f'<div class="stage-rail">{"".join(nodes)}</div>', unsafe_allow_html=True)


def render_step_position(steps: list[dict[str, Any]], current_index: int) -> None:
    labels = []
    for index, step in enumerate(steps):
        state = "active" if index == current_index else "passed" if index < current_index else ""
        labels.append(
            f'<span class="step-tick {state}"><b>{index + 1}</b>{html.escape(_step_title(step))}</span>'
        )
    st.markdown(f'<div class="step-ticks">{"".join(labels)}</div>', unsafe_allow_html=True)


def _render_table(rows: list[dict[str, Any]]) -> None:
    frame = pd.DataFrame(rows)
    height = min(330, 38 * (len(frame.index) + 1) + 6)
    st.dataframe(frame, width="stretch", height=height, hide_index=True)


def render_execution_diagram() -> None:
    st.markdown('<div class="section-tag diagram-tag">SƠ ĐỒ NGUYÊN LÝ</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div class="execution-diagram" aria-label="Một Job gồm nhiều Stage, mỗi Stage gồm nhiều Task, mỗi Task xử lý một phần dữ liệu">
          <div class="diagram-node"><strong>1 Job</strong><span>Một đợt chạy</span></div>
          <div class="diagram-arrow" aria-hidden="true">→</div>
          <div class="diagram-node"><strong>Nhiều Stage</strong><span>Mỗi chặng xử lý</span></div>
          <div class="diagram-arrow" aria-hidden="true">→</div>
          <div class="diagram-node"><strong>mỗi Stage có nhiều Task</strong><span>Các việc nhỏ chạy song song</span></div>
          <div class="diagram-arrow" aria-hidden="true">→</div>
          <div class="diagram-node"><strong>Mỗi Task</strong><span>Xử lý một phần dữ liệu</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status(step: dict[str, Any], fallback: bool) -> None:
    status = str(step.get("status", "pending"))
    if status == "failed":
        error = step.get("error", {}) or {}
        st.error(f"Bước này lỗi: {error.get('message', 'Không rõ nguyên nhân')}.")
        if error.get("recovery_hint"):
            st.caption(f"Cách chạy lại: {error['recovery_hint']}")
    elif status == "running":
        st.warning("Spark đang chạy bước này.")
    elif status == "pending":
        st.info("Bước này chưa được chạy.")
    elif fallback:
        st.caption("Dữ liệu minh họa từ lần chạy gần nhất.")


def render_technical_details(step: dict[str, Any]) -> None:
    evidence = step.get("evidence", {}) or {}
    result = step.get("result", {}) or {}
    plan = evidence.get("physical_plan") or result.get("physical_plan")
    pipeline = evidence.get("pipeline_stage_names") or result.get("pipeline_stage_names")
    checks = evidence.get("restart_checks")

    with st.expander("Chi tiết kỹ thuật — mở khi cần"):
        has_detail = False
        if plan:
            has_detail = True
            st.markdown("**Physical plan rút gọn**")
            st.code(str(plan)[-3500:], language="text")
        if pipeline:
            has_detail = True
            st.markdown("**Các bước trong MLlib Pipeline**")
            st.code(" → ".join(str(item) for item in pipeline), language="text")
        if checks:
            has_detail = True
            st.markdown("**Kiểm tra checkpoint**")
            st.json(checks)
        if not has_detail:
            st.caption("Không có chi tiết bổ sung cho bước này.")


def render_step(step: dict[str, Any], *, fallback: bool = False) -> None:
    index = int(step.get("index", 1))
    view = build_step_view(step)
    stage = view["stage"]

    st.markdown(
        f'<div class="step-heading"><span>Giai đoạn {html.escape(str(stage["number"]))} · Bước {index}/7</span>'
        f'<strong>{html.escape(_step_title(step))}</strong></div>',
        unsafe_allow_html=True,
    )
    render_status(step, fallback)

    st.markdown('<div class="section-tag">CÂU HỎI CẦN TRẢ LỜI</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="lab-question">{html.escape(view["question"])}</div>', unsafe_allow_html=True)

    input_column, spark_column = st.columns([1.45, 1], gap="large")
    with input_column:
        st.markdown('<div class="section-tag">DỮ LIỆU ĐẦU VÀO</div>', unsafe_allow_html=True)
        _render_table(view["input_table"])
    with spark_column:
        st.markdown('<div class="section-tag spark-tag">SPARK ĐANG LÀM GÌ?</div>', unsafe_allow_html=True)
        st.code("\n".join(view["spark_action"]), language="python")

    if view["term_table"]:
        st.markdown('<div class="section-tag spark-tag">JOB / STAGE / TASK LÀM GÌ?</div>', unsafe_allow_html=True)
        _render_table(view["term_table"])
        render_execution_diagram()

    result_label = "SỐ LIỆU CỦA LẦN CHẠY NÀY" if view["term_table"] else "KẾT QUẢ"
    st.markdown(f'<div class="section-tag result-tag">{result_label}</div>', unsafe_allow_html=True)
    _render_table(view["result_table"])
    st.markdown(
        f'<div class="reading-note"><b>Cách đọc bảng:</b> {html.escape(view["reading_hint"])}</div>',
        unsafe_allow_html=True,
    )

    conclusion, ids = st.columns(2, gap="large")
    with conclusion:
        st.markdown('<div class="section-tag">ĐIỀU VỪA CHỨNG MINH</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="plain-answer">{html.escape(view["conclusion"])}</div>', unsafe_allow_html=True)
    with ids:
        st.markdown('<div class="section-tag ids-tag">LIÊN HỆ VỚI IDS</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="plain-answer ids-answer">{html.escape(view["ids_link"])}</div>', unsafe_allow_html=True)

    render_technical_details(step)


def render_navigation(current_index: int, total_steps: int, spark_ui_url: str | None, spark_ui_live: bool) -> None:
    back, spark_column, next_column = st.columns([1, 1.15, 1])
    with back:
        if st.button("Quay lại", disabled=current_index <= 0, use_container_width=True):
            st.session_state.demo_step = max(0, current_index - 1)
            st.rerun()
    with spark_column:
        if spark_ui_url and spark_ui_live:
            st.link_button("Mở Spark UI", spark_ui_url, use_container_width=True)
        else:
            st.button("Spark UI đã dừng", disabled=True, use_container_width=True)
    with next_column:
        if st.button("Tiếp theo", type="primary", disabled=current_index >= total_steps - 1, use_container_width=True):
            st.session_state.demo_step = min(total_steps - 1, current_index + 1)
            st.rerun()


def render_guided_demo(status: dict[str, Any]) -> None:
    steps = status.get("steps", [])
    if not steps:
        st.error("Không có dữ liệu demo.")
        return
    current_index = int(st.session_state.get("demo_step", 0))
    current_index = min(max(current_index, 0), len(steps) - 1)
    st.session_state.demo_step = current_index
    current_step = steps[current_index]

    render_stage_rail(str(current_step.get("id", "")))
    render_step_position(steps, current_index)
    render_step(current_step, fallback=bool(status.get("fallback")))
    render_navigation(
        current_index,
        len(steps),
        status.get("spark_ui_url"),
        spark_ui_live=status.get("status") == "running",
    )

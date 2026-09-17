# Data Lab Story Dashboard Implementation Plan

> **For Codex:** Implement this plan task-by-task with test-driven development.

**Goal:** Replace the metric-card dashboard with a concise, table-first Spark teaching story organized into three stages and seven steps.

**Architecture:** Keep the current Streamlit app and demo artifact contract. Add pure presentation-model helpers that translate each artifact step into short questions, input/result tables, Spark actions, conclusions, and IDS links; render those models with a restrained Data Engineering Lab visual system.

**Tech Stack:** Python, Streamlit, pandas, Streamlit AppTest, pytest, HTML/CSS.

**Constraint:** Do not run Git commands or create commits unless the user gives fresh permission.

---

### Task 1: Presentation model

**Files:**
- Modify: `tests/test_dashboard.py`
- Create: `dashboard/presentation.py`

1. Add failing tests for the three stage definitions and seven step presentation models.
2. Run the focused tests and verify failure because the new module is missing.
3. Implement pure helpers for stage lookup, short copy, tables, conclusions, and Spark snippets.
4. Run focused tests until green.

### Task 2: Table-first Streamlit renderer

**Files:**
- Modify: `tests/test_dashboard.py`
- Modify: `dashboard/guided_demo.py`
- Modify: `dashboard/app.py`

1. Add failing AppTest assertions for stage navigation, question text, table-first labels, concise conclusions, and seven-step navigation.
2. Run the focused tests and verify the old renderer fails them.
3. Replace the flow cards and detached metrics with a fixed teaching sequence: question, input, Spark operation, result, conclusion, IDS link.
4. Keep physical plans and raw technical evidence in optional expanders.
5. Run focused tests until green.

### Task 3: Data Engineering Lab visual system

**Files:**
- Modify: `tests/test_dashboard.py`
- Modify: `dashboard/styles.py`

1. Add source-level assertions for three-stage rail and removal of generic metric-card rendering.
2. Verify failure against the old styles and renderer.
3. Implement the industrial editorial layout, table hierarchy, orange Spark rail, restrained type, responsive rules, and visible keyboard focus.
4. Run dashboard tests until green.

### Task 4: Regression and visual QA

**Files:**
- Modify only files above if defects are found.

1. Run `pytest tests/test_dashboard.py -q`.
2. Run the full test suite.
3. Reload the running Streamlit app and inspect all seven steps at desktop width.
4. Inspect one narrow viewport and fix clipping or hierarchy issues.
5. Leave the dashboard running at `http://127.0.0.1:8501` for user review.

# Execution Story Implementation Plan

> **For Codex:** Implement task-by-task using test-driven development. Do not use Git without separate user permission.

**Goal:** Make Job/Stage/Task understandable through the actual network-flow query and keep the MLlib/Streaming application concise.

**Architecture:** Extend the pure `dashboard/presentation.py` view model with a three-row terminology table and artifact-backed execution facts. Render the extra table only on the execution step. Adjust Stage 3 copy to identify the educational MLlib sample and file-based streaming simulation; retain the existing Streamlit navigation and artifact contract.

**Tech Stack:** Python 3.12, Streamlit, pandas, pytest, Streamlit AppTest.

---

### Task 1: Correct execution facts

**Files:** `tests/test_dashboard.py`, `dashboard/presentation.py`

1. Add a failing test with a small execution fixture asserting 3 terminology rows, real Job/Stage/Task counts, separate declared/completed task labels, and no fabricated zeros when tracker data is absent.
2. Run `\.venv\Scripts\python.exe -m pytest tests\test_dashboard.py -q` and confirm it fails for missing view fields.
3. Implement only the view fields and table rows required by the test.
4. Re-run the focused tests until green.

### Task 2: Render the explanation in the main flow

**Files:** `tests/test_dashboard.py`, `dashboard/guided_demo.py`, `dashboard/styles.py`

1. Add a failing AppTest assertion for the execution step's “Job / Stage / Task làm gì?” heading and visible three-row table.
2. Run the focused test and confirm it fails against the old renderer.
3. Render the table between the Spark action and result table, only for execution.
4. Style it as part of the existing industrial editorial data path, with no new KPI cards.
5. Re-run dashboard tests.

### Task 3: Stage 3 as short IDS application

**Files:** `tests/test_dashboard.py`, `dashboard/presentation.py`

1. Add failing tests asserting MLlib is explicitly labeled an educational validation sample, and Streaming explicitly labeled a file-based simulation, not packet capture.
2. Verify the tests fail.
3. Update only the short copy, input labels and reading hints necessary.
4. Re-run dashboard tests.

### Task 4: Verification

1. Run dashboard tests and any cheap adjacent tests.
2. Run the complete test suite if time permits; it takes about 8–9 minutes on this machine.
3. Check the live dashboard at `http://127.0.0.1:8501` if running; otherwise start it and inspect stages 2 and 3.
4. Report exact results and note that Git was not used.

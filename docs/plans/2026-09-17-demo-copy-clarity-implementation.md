# Demo Copy Clarity Implementation Plan

> **For Codex:** Execute these tasks in order. User previously prohibited Git without separate permission. Do not run Git commands. Keep the web server stopped unless the user asks to view it again.

**Goal:** Make every step understandable to a first-time Spark viewer, add one clear Job/Stage/Task diagram, and ensure every displayed conclusion matches the artifact of the selected run.

**Architecture:** Keep the seven-step Streamlit layout and its artifact contract. Build short, run-aware teaching text in `dashboard/presentation.py`; `dashboard/guided_demo.py` renders the text and one small code-native diagram only on the execution step. Change the hero in `dashboard/app.py` to define the one term the user needs before step 1.

**Tech Stack:** Python 3.12, Streamlit, pandas, pytest, Streamlit AppTest.

**Project root:** `D:\Learning\bigdata\SparkNIDS-Spark-Network-Intrusion-Detection-System`

---

### Task 1: Make the first screen self-explanatory

**Files:** Modify `dashboard/app.py`; test `tests/test_dashboard.py`.

1. Add an AppTest assertion that the hero defines `flow` in one short sentence, e.g. `Flow là một bản ghi tóm tắt hoạt động mạng.`
2. Run the focused test and confirm it fails before the change.
3. Add the sentence immediately beneath the title; keep the title and stage rail intact.
4. Rerun the test and inspect the first-screen reading order. Do not add a large glossary above the demo.

### Task 2: Correct labels and mixed units in the result tables

**Files:** Modify `dashboard/presentation.py`; test `tests/test_dashboard.py`.

1. Add a view-model test that CSV/Parquet and Cache show a clear column heading such as `Thời gian chạy (giây)` and use one data type in that column. Explain that CSV/Parquet and cache-reuse rows report the median of repeated runs, while `Nạp cache lần đầu` is a separate one-time measurement.
2. Add a test that Lazy Evaluation keeps Job counts and action duration in separate columns or tables. The current `Số Job` column also contains `0.239 giây`, which mixes count with duration.
3. Run the tests to confirm the old labels and mixed column fail.
4. Update the table builders without changing benchmark values; use ordinary Vietnamese in reading hints.
5. Rerun focused tests.

### Task 3: Make Job/Stage/Task claims follow the selected run

**Files:** Modify `dashboard/presentation.py`; test `tests/test_dashboard.py`.

1. Build two test fixtures with different tracker counts and one fixture with no tracker. Assert that the reading hint never contains the old fixed `20 Task`, `7` or an invented zero when the artifact differs or is missing.
2. Run the tests to confirm the fixed sentence fails.
3. Compose the hint from `status_tracker` values. Suggested wording when counts exist: `Spark ghi nhận {jobs} Job, {stages} Stage và {created} Task được tạo; {completed} Task hoàn tất, {failed} Task lỗi.` Add a short follow-up that planned tasks in skipped adaptive stages need not finish. When values are missing, explain only the Job/Stage/Task relationship.
4. Change `Lần chuyển dữ liệu` to a technically accurate description of `Exchange` occurrences in the physical-plan text, or move this number into the existing technical expander. Do not present it as an actual transfer count.
5. Rerun focused tests against both fixtures and the saved demo status.

### Task 4: Add one conceptual Job/Stage/Task diagram

**Files:** Modify `dashboard/guided_demo.py`, `dashboard/styles.py`; test `tests/test_dashboard.py`.

1. Add a focused AppTest assertion that the execution step alone shows a diagram headed `Sơ đồ nguyên lý` before its measured result table. Other six steps must not show it.
2. Run the focused test and confirm the diagram is absent.
3. Render a small, static, accessible flow: `1 Job` → `nhiều Stage` → `mỗi Stage có nhiều Task` → `mỗi Task xử lý một phần dữ liệu`. Use HTML/CSS from the existing dashboard, not a raster image or animation. Keep labels readable as text and explain arrows in the markup.
4. Place it after the Job/Stage/Task explanation and before the result table. Label the existing table `Số liệu của lần chạy này` so the conceptual diagram cannot be mistaken for the measured 4 Job, 8 Stage and 20 declared Task.
5. Check desktop and narrow viewport layout: boxes must wrap or stack without horizontal scrolling. Keep the diagram visually quieter than the result table.
6. Rerun the focused AppTest.

### Task 5: Let the Cache conclusion state what the numbers show

**Files:** Modify `dashboard/presentation.py`; test `tests/test_dashboard.py`.

1. Add tests for cached time slower than uncached, faster than uncached, and missing times.
2. Confirm the current generic conclusion fails at least the slower case.
3. Derive a one-sentence conclusion from `without_cache.median_seconds`, `with_cache.median_seconds`, and `cache_materialization_seconds`. For the current run it should say that reuse with cache took longer (`0.347` vs `0.279` seconds) and loading it first cost `1.745` seconds. For a different run, describe its actual result. If data is missing, avoid claiming a winner.
4. Keep the general rule about repeat use as a separate short sentence; do not claim cache always helps.
5. Rerun focused tests.

### Task 6: Explain MLlib and Streaming without implying one continuous model

**Files:** Modify `dashboard/presentation.py`; test `tests/test_dashboard.py`.

1. Add tests using the saved demo artifact: MLlib is an educational model trained on a sample and not saved; Streaming loads the project's saved final model. Assert the copy distinguishes them.
2. Confirm the existing copy fails this assertion.
3. Show a compact relationship at the start of step 7: `Bước trước minh họa cách huấn luyện; bước này dùng mô hình đồ án đã lưu để phân loại flow mới.` Source this from the artifact where possible; do not assert the same model was handed from step 6 to step 7.
4. Replace first-use jargon in visible copy: `schema` → `cấu trúc cột (schema)`, `partition` → `phần dữ liệu (partition)`, `micro-batch` → `đợt xử lý nhỏ (micro-batch)`, `checkpoint` → `dấu mốc ghi nhớ đã đọc đến đâu`. Keep code tokens unchanged inside the Spark action snippet and technical expander.
5. Clarify that each **row in the results table** represents one batch, not that each flow row is one batch.
6. Rerun focused tests.

### Task 7: Verify the seven-step story

**Files:** `tests/test_dashboard.py`; optionally `dashboard/guided_demo.py` only if render changes are needed.

1. Run `\.venv\Scripts\python.exe -m pytest tests\test_dashboard.py -q` from the project root. Expected: all dashboard tests pass with no Streamlit exception.
2. Use Streamlit AppTest to step through all seven views and assert that each has input, Spark action, result and a one-sentence conclusion. Verify the current saved run's Cache and Execution statements against the displayed values.
3. Check one alternate run fixture and the offline fallback for missing values; no hardcoded current-run numbers should leak into them.
4. Do a manual copy pass: each unfamiliar term is defined at first use, no table column mixes counts and seconds, stage 3 clearly distinguishes the educational model from the final model, and the diagram is visibly separated from measured numbers.
5. Report the exact test result. Do not restart the web server merely for this review; it was intentionally turned off at the user's request.

**Done when:** A viewer can explain what each table proves without guessing terminology, can read the conceptual Job/Stage/Task flow without mistaking it for measured counts, and sees numbers and conclusions that agree for the current run, another run and missing data; all dashboard tests pass.

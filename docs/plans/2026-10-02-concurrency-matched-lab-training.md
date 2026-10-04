# Concurrency-Matched Live Lab Training Plan

> **For Codex:** Execute these steps in order in the current workspace. Do not use Git; the user has not authorized Git operations.

**Goal:** Make Live Lab training include balanced concurrent traffic so the MLlib model sees conditions similar to the 15-sender demo.

**Architecture:** Keep the current session features and Random Forest. Change the training generator so each batch contains five normal, five scan, and five burst runs executed concurrently with Python's standard-library thread pool. For Live Lab only, break ties between equally good validation thresholds by selecting the lower threshold. Verify through tests, collect concurrent training data, retrain only the Live Lab model, and rerun the 15-sender experiment.

**Tech Stack:** Python standard library, pytest, Docker Compose, PySpark MLlib, Streamlit.

---

### Task 1: Prove training traffic is concurrent and balanced

**Files:**
- Modify: `tests/test_live_lab.py`
- Production code under test: `lab/generator.py`

**Step 1: Write a failing test**

Patch `lab.generator.generate` with a thread-safe observer that waits on a 15-party `threading.Barrier`. Call `generate_training(..., runs_per_scenario=5)` and assert the generator created five runs for each scenario and 15 total flows. The current serial implementation must fail at the barrier.

**Step 2: Run the focused test and confirm RED**

Run: `.\.venv\Scripts\pytest.exe tests\test_live_lab.py -k training_generator -q --basetemp tmp\pytest-concurrency-red`

Expected: fail because 15 training tasks are not active concurrently.

### Task 2: Generate concurrent training batches

**Files:**
- Modify: `lab/generator.py`

**Step 1: Implement the smallest change**

Use `concurrent.futures.ThreadPoolExecutor` to run one batch of 15 training runs at a time: five per scenario. Repeat batches until `runs_per_scenario` runs of each scenario have been generated. Validate that the requested per-scenario count is a positive multiple of five; keep existing random ranges, labels, run IDs, and truth files.

**Step 2: Run the focused test and confirm GREEN**

Run: `.\.venv\Scripts\pytest.exe tests\test_live_lab.py -k training_generator -q --basetemp tmp\pytest-concurrency-green`

Expected: pass.

### Task 3: Verify the Live Lab regression suite

**Files:**
- No additional changes expected.

Run: `.\.venv\Scripts\pytest.exe tests\test_live_lab.py tests\test_lab_model.py tests\test_lab_stream.py -q --basetemp tmp\pytest-concurrency-suite`

Expected: all targeted tests pass.

### Task 4: Retrain and validate on concurrent Docker traffic

**Files:**
- Generated data: `data/lab/incoming/training`, `data/lab/truth/runs`
- Model/report: `models/live_lab/session_pipeline`, `models/live_lab/metrics.json`

Run the generator in the isolated Compose lab to append 90 balanced concurrent training runs. Call the existing `train_session_model` on the resulting Docker training data and write its report to `models/live_lab/metrics.json`; do not run the full UNSW trainer because this workspace does not contain the UNSW Silver train/test Parquet inputs.

Restart the local Spark scoring stream so it loads the new model. Rerun the 15-sender demo and verify all senders complete, Spark scores all runs, and the actual-versus-predicted counts are displayed. Report any remaining misses or false alarms honestly; do not claim the controlled Docker score is official UNSW-NB15 performance.

### Task 5: Select a more sensitive tied threshold for Live Lab

**Files:**
- Modify: `src/lab_model.py`
- Test: `tests/test_lab_model.py`

If validation shows multiple thresholds with the same best F1, recall, and false-positive rate, select the lowest one for the Live Lab session model. Keep the shared threshold selector and the separate UNSW model policy unchanged. Add a unit test proving a worse low-threshold candidate is rejected while the lowest equally good threshold wins. Retrain the lab model, restart streaming, and confirm the fresh 15-sender run has no false positives or missed scripted attacks.

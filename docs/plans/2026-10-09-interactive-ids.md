# Interactive Spark IDS Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Show how changing real Docker traffic produces observed features, MLlib decisions and independently checked alerts.

**Architecture:** Fifteen bounded, persistent Docker senders accept atomic control files. The victim batches measured TCP flows into JSONL. Spark maintains 10-second source-IP windows with a 30-second event-time watermark, scores updates, and publishes small atomic snapshots. Separate Docker experiments supply train, validation and test; UNSW-NB15 provides a distinct scale experiment.

**Tech Stack:** Existing Python, Docker Compose, Spark SQL/Structured Streaming/MLlib, Streamlit, pandas and Altair. No additional services or packages.

## Accepted decisions and assumptions

- Keep 15 senders on the isolated Docker network. This is a classroom lab on one computer, Spark local[2], not a distributed cluster benchmark.
- Live changes take effect within a short sending cycle; model updates depend on measured Spark batch time, not a promised fixed latency.
- Model features exclude actor ID, experiment ID, run ID, scenario and truth labels. Source IP and experiment scope identify windows only.
- A mixed window containing attack-labeled events is positive. Partial windows are provisional; verdicts are checked only after the lateness allowance and independently recorded truth agree.
- Benchmark actual original UNSW rows, repeated measurements, same aggregate and signatures; never fabricate additional independent samples or promise speedup.
- Preserve previous source and generated data. Do not use Git without a new explicit request.
- Visual direction: restrained industrial console, shared warm paper/ink/orange palette, clear network topology and short controls. DFII: impact 4, fit 5, feasibility 5, performance 5, consistency risk 3 = 16 (strong fit; capped at skill's 15-point interpretation).

## Tasks

1. Write behavior checks in tests/test_live_windows.py; confirm missing window grouping, controls and separate truth handling fail. Reuse tests/test_live_lab.py for existing victim compatibility.
2. Add aggregate_windows to src/lab_features.py. Count ports using collect_set in streaming state; group by observed source IP, experiment and event-time window; preserve aggregate_sessions.
3. Extend lab/generator.py with live profiles, bounded pacing, timed automatic phases and manual control; batch victim writes in lab/victim.py. Preserve original finite CLI modes.
4. Extend dashboard/live_lab_controller.py with live startup, atomic per-machine commands, stream lifecycle and inspection of sender state. Keep finite start_experiment API for compatibility.
5. Add src/lab_window_model.py: collect independently seeded Docker experiments, train a window model, select threshold on validation, report untouched test and simple-rule baseline. Save under models/live_lab/window_pipeline only.
6. Replace src/lab_stream.py live execution with incremental window updates and versioned checkpoint/output. Persist idempotent window upserts plus Spark progress, not raw-input rescans.
7. Add src/lab_scale.py for the same UNSW aggregate at increasing original-data sizes; record repeat medians, signatures, partition/task evidence, local execution and input size. Expose a run button and readable results.
8. Rework dashboard/pages/01_Live_Lab.py into network, interactive scenario, live timeline, alert detail and checked results. Reuse dashboard/styles.py palette; connect dashboard/app.py to the live experience.
9. Update docs/LIVE_LAB_GUIDE.md, README.md and scripts/run_live_lab.ps1. Compile modules, run relevant tests, collect/train with Docker, exercise automatic/manual transitions and restart checkpoint, then inspect browser.

## Verification

- .venv/Scripts/python.exe -m pytest tests/test_live_windows.py tests/test_live_lab.py tests/test_lab_stream.py -q -p no:cacheprovider --basetemp=outputs/live_lab/test-temp
- .venv/Scripts/python.exe -m src.lab_window_model --collect --duration 50
- .venv/Scripts/python.exe -m src.lab_stream
- .venv/Scripts/python.exe -m src.lab_scale
- Streamlit runtime check plus actual TCP Docker demo: normal -> scan/burst -> normal; controls must alter measured features, snapshots must have no labels, display must explain provisional results.
- Restart stream on its versioned checkpoint and verify windows are upserted without duplicates.

## Ownership and limits

One maintainer can run the project from the existing virtual environment. Controller-owned processes have explicit start/stop; snapshots and logs remain recoverable for presentation. TCP/application measurements and controlled scenario labels describe this lab only. Inspect test errors honestly; no requirement for 100% accuracy. External Internet traffic and firewall enforcement are outside this change.

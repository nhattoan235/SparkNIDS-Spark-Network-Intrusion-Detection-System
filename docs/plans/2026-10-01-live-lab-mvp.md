# Live Lab: session-level MLlib implementation

**Goal:** Show isolated Docker traffic being grouped and classified by Apache Spark MLlib, with results that can be repeated and checked on separate runs.

**Architecture:** The victim writes one JSON file per TCP flow with a generated `run_id`; training and demo flow files use separate folders, while scenario labels stay in separate run-truth files. The generator creates varied training runs and three interactive demo runs. Spark Structured Streaming watches only demo files, recomputes session features from recorded demo flows, scores them with a Random Forest trained on Docker training runs, and writes a small dashboard snapshot. The existing UNSW-NB15 model and official test report remain a separate part of the project.

**Tech stack:** Python standard library, Docker Compose, PySpark 4.2, Spark MLlib, Streamlit.

**Limits:** The victim measures application-level connections, not packet captures. Training/test numbers for the live model describe controlled lab scenarios. Spark runs `local[2]`; session aggregation rescans the small demo folder on each update and must be replaced by incremental state for larger traffic. The main UNSW-NB15 pipeline remains the large labeled-data experiment.

## Acceptance criteria

- The Docker training generator creates enough complete normal, scan, and burst runs to split by `run_id` into train, validation, and test; training files do not enter the live stream.
- Spark builds the same documented session features for training and live scoring; labels, scenario, and run mode do not enter the model features.
- The threshold is selected on validation; final lab metrics are calculated only on held-out runs.
- The live dashboard shows one row per run, flow counts, destination ports, flow rate, model decision, score, and confusion counts after truth is joined.
- The three Docker demo commands work on the internal-only network and the run instructions explain what the viewer should see.
- Existing offline dashboard and UNSW-NB15 pipeline remain intact.
- User explicitly said not to use Git unless permitted; implementation and verification use no Git commands.

## Implementation order

1. Add tests for shared Spark session features, run metadata, training split integrity, and session model scoring.
2. Extend generator/victim records with a run ID and a training/demo mode while keeping truth in its own directory.
3. Add shared Spark aggregation features and train a session-level Random Forest from complete labeled training runs.
4. Update Structured Streaming to score session aggregates and write an overwrite-safe snapshot under a new output/checkpoint path.
5. Simplify the Live Lab page around the traffic → Spark aggregation → MLlib decision flow.
6. Update the walkthrough and run the relevant unit/Spark tests plus an end-to-end Docker demo.

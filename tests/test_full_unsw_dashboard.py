from __future__ import annotations

import json
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from dashboard.full_unsw_access import load_full_unsw_status


def test_full_data_status_is_missing_until_a_real_run_exists(tmp_path: Path):
    status = load_full_unsw_status(tmp_path)

    assert status["status"] == "missing"
    assert status["ready"] is False
    assert status["run"] == {}


def test_full_data_status_refuses_run_path_outside_data_root(tmp_path: Path):
    (tmp_path / "latest.json").write_text(
        json.dumps({"schema_version": 1, "run_id": "bad", "run_path": str(tmp_path.parent / "outside.json")}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="inside the full UNSW data root"):
        load_full_unsw_status(tmp_path)


def test_main_page_explains_four_stages_without_claiming_missing_results(monkeypatch, tmp_path):
    monkeypatch.setenv("SPARKNIDS_FULL_UNSW_ROOT", str(tmp_path))

    app = AppTest.from_file(
        str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py"),
        default_timeout=30,
    ).run()

    assert not app.exception
    content = "\n".join([item.value for item in app.markdown] + [item.value for item in app.caption])
    assert "2.54 triệu" in content
    assert "2,540,044" in content and "2,540,047" in content
    assert "UNSW-NB15" in content
    assert "Spark MLlib" in content
    assert "Dữ liệu" in content and "Spark" in content and "MLlib" in content and "Kết quả" in content
    assert "chưa có kết quả" in content.lower()
    assert any(button.label == "Chạy pipeline UNSW đầy đủ" for button in app.button)


def test_main_page_shows_real_test_counts_and_metrics(monkeypatch, tmp_path):
    run_dir = tmp_path / "runs" / "fixture-run"
    run_dir.mkdir(parents=True)
    run = {
        "schema_version": 1,
        "run_id": "fixture-run",
        "status": "succeeded",
        "source_manifest": {"total_bytes": 586377596, "file_count": 4},
        "quality_before_cleaning": {"rows": 2540044},
        "splits": {
            "train": {"rows": 100},
            "validation": {"rows": 20},
            "test": {"rows": 30},
        },
        "model_lock": {"model": "Spark MLlib LogisticRegression", "fit_rows": 100, "decision_threshold": 0.4},
        "test": {
            "rows": 30,
            "prediction_rows": 30,
            "metrics": {
                "true_positive": 10,
                "false_positive": 2,
                "true_negative": 13,
                "false_negative": 5,
                "precision": 10 / 12,
                "recall": 10 / 15,
                "f1": 20 / 27,
                "false_positive_rate": 2 / 15,
            },
            "examples": [],
        },
    }
    (run_dir / "run.json").write_text(json.dumps(run), encoding="utf-8")
    (tmp_path / "latest.json").write_text(
        json.dumps({"schema_version": 1, "run_id": "fixture-run", "status": "succeeded", "run_path": str(run_dir / "run.json")}),
        encoding="utf-8",
    )
    monkeypatch.setenv("SPARKNIDS_FULL_UNSW_ROOT", str(tmp_path))

    app = AppTest.from_file(
        str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py"),
        default_timeout=30,
    ).run()

    assert not app.exception
    content = "\n".join(item.value for item in app.markdown)
    metric_labels = [metric.label for metric in app.metric]
    metric_values = [metric.value for metric in app.metric]
    assert "Flow test đã chấm" in metric_labels
    assert "30" in metric_values
    assert "Bắt đúng tấn công" in metric_labels
    assert "Bỏ sót tấn công" in metric_labels
    assert any(button.label == "Chạy pipeline UNSW đầy đủ" for button in app.button)

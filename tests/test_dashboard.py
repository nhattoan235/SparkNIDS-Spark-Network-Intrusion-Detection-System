"""Phase-9 tests for bounded offline artifacts and Streamlit rendering."""

from __future__ import annotations

from pathlib import Path
import json

import pytest
from streamlit.testing.v1 import AppTest

from dashboard.data_access import (
    PROJECT_ROOT,
    load_dashboard_bundle,
    load_demo_status,
    load_static_demo_fallback,
)


def test_dashboard_bundle_is_offline_and_bounded() -> None:
    bundle = load_dashboard_bundle()
    manifest = bundle["manifest"]
    alerts = bundle["alerts"]

    assert manifest["offline_ready"] is True
    assert manifest["large_dataset_loaded_by_dashboard"] is False
    assert len(alerts["alerts"]) <= alerts["limits"]["alerts"]
    assert len(alerts["false_positives"]) <= alerts["limits"]["errors_per_type"]
    assert len(alerts["false_negatives"]) <= alerts["limits"]["errors_per_type"]
    assert alerts["collection_policy"]["full_prediction_dataset_collected"] is False
    assert alerts["collection_policy"]["to_pandas_used"] is False


def test_dashboard_source_never_calls_spark_to_pandas() -> None:
    sources = [PROJECT_ROOT / "dashboard" / "app.py", PROJECT_ROOT / "src" / "export_dashboard_data.py"]
    for path in sources:
        assert ".toPandas(" not in path.read_text(encoding="utf-8")


def test_streamlit_app_renders_without_exception() -> None:
    app = AppTest.from_file(str(Path(PROJECT_ROOT) / "dashboard" / "app.py"), default_timeout=30).run()

    assert not app.exception
    assert app.title or app.markdown
    rendered_text = list(app.markdown) + list(app.subheader)
    assert any("Dữ liệu và schema" in item.value for item in rendered_text)
    assert len(app.sidebar.radio) == 0
    assert len(app.metric) <= 3

    next_button = next(item for item in app.button if item.label == "Tiếp theo")
    next_button.click().run()
    assert not app.exception
    rendered_text = list(app.markdown) + list(app.subheader)
    assert any("CSV và Parquet" in item.value for item in rendered_text)

    back_button = next(item for item in app.button if item.label == "Quay lại")
    back_button.click().run()
    assert not app.exception
    rendered_text = list(app.markdown) + list(app.subheader)
    assert any("Dữ liệu và schema" in item.value for item in rendered_text)

    for _ in range(6):
        next(item for item in app.button if item.label == "Tiếp theo").click().run()
        assert not app.exception
    rendered_text = list(app.markdown) + list(app.subheader)
    assert any("Structured Streaming" in item.value for item in rendered_text)


def test_load_demo_status_follows_latest_pointer(tmp_path: Path) -> None:
    demo_root = tmp_path / "demo"
    run_dir = demo_root / "runs" / "fixture"
    run_dir.mkdir(parents=True)
    source = PROJECT_ROOT / "tests" / "fixtures" / "demo_status_complete.json"
    (run_dir / "status.json").write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    (demo_root / "latest.json").write_text(
        json.dumps({"schema_version": 1, "status_path": "runs/fixture/status.json"}),
        encoding="utf-8",
    )

    status = load_demo_status(demo_root)

    assert status["run_id"] == "fixture-complete"
    assert status["status"] == "succeeded"


def test_load_demo_status_rejects_path_outside_demo_root(tmp_path: Path) -> None:
    demo_root = tmp_path / "demo"
    demo_root.mkdir()
    (demo_root / "latest.json").write_text(
        json.dumps({"schema_version": 1, "status_path": "../outside.json"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="inside demo output root"):
        load_demo_status(demo_root)


def test_static_fallback_contains_seven_steps() -> None:
    status = load_static_demo_fallback()

    assert status["fallback"] is True
    assert [step["id"] for step in status["steps"]] == [
        "data", "parquet", "lazy", "execution", "cache", "mllib", "streaming"
    ]

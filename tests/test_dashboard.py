"""Phase-9 tests for bounded offline artifacts and Streamlit rendering."""

from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest

from dashboard.data_access import PROJECT_ROOT, load_dashboard_bundle


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
    assert any("Tổng quan dữ liệu" in item.value for item in app.subheader)

    for page in ("Mô hình", "Cảnh báo", "Spark benchmark"):
        app.radio[0].set_value(page).run()
        assert not app.exception, page

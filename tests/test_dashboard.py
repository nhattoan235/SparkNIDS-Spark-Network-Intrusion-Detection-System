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
from dashboard.presentation import STAGES, build_step_view


def test_presentation_groups_seven_steps_into_three_stages() -> None:
    assert [stage["title"] for stage in STAGES] == [
        "Chuẩn bị dữ liệu",
        "Spark xử lý phân tán",
        "Phát hiện xâm nhập",
    ]
    assert [step_id for stage in STAGES for step_id in stage["step_ids"]] == [
        "data",
        "parquet",
        "lazy",
        "execution",
        "cache",
        "mllib",
        "streaming",
    ]


def test_each_step_view_has_a_short_teaching_story() -> None:
    status = load_demo_status()

    for step in status["steps"]:
        view = build_step_view(step)
        assert view["question"].endswith("?")
        assert 1 <= len(view["spark_action"]) <= 3
        assert view["input_table"]
        assert view["result_table"]
        assert 1 <= len(view["reading_hint"]) <= 180
        assert len(view["conclusion"]) <= 180
        assert len(view["ids_link"]) <= 180


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
    page_text = "\n".join(item.value for item in app.markdown)
    assert "SPARK IDS LAB" in page_text
    assert "Một flow mạng đi qua Spark như thế nào?" in page_text
    assert "Flow là một bản ghi tóm tắt một lượt giao tiếp mạng." in page_text
    assert "Chuẩn bị dữ liệu" in page_text
    assert "Spark xử lý phân tán" in page_text
    assert "Phát hiện xâm nhập" in page_text
    assert "CÂU HỎI CẦN TRẢ LỜI" in page_text
    assert "DỮ LIỆU ĐẦU VÀO" in page_text
    assert "SPARK ĐANG LÀM GÌ?" in page_text
    assert "KẾT QUẢ" in page_text
    assert "LIÊN HỆ VỚI IDS" in page_text
    assert len(app.sidebar.radio) == 0
    assert len(app.metric) == 0
    assert len(app.dataframe) >= 2

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


def test_guided_renderer_does_not_use_generic_metric_cards() -> None:
    source = (PROJECT_ROOT / "dashboard" / "guided_demo.py").read_text(encoding="utf-8")

    assert ".metric(" not in source


def test_styles_define_the_data_lab_visual_hierarchy() -> None:
    source = (PROJECT_ROOT / "dashboard" / "styles.py").read_text(encoding="utf-8")

    for selector in (".stage-rail", ".lab-question", ".section-tag", ".reading-note", ":focus-visible"):
        assert selector in source


def test_execution_view_explains_work_without_mislabeling_tasks() -> None:
    step = {
        "id": "execution",
        "result": {
            "input_partitions": 4,
            "output_aggregate_rows": 6,
            "shuffle_exchange_nodes": 2,
            "status_tracker": {
                "job_count": 4,
                "unique_stage_count": 8,
                "total_tasks_across_unique_stages": 20,
                "completed_tasks_across_unique_stages": 7,
                "failed_tasks_across_unique_stages": 0,
            },
        },
    }

    view = build_step_view(step)

    assert [row["Spark gọi"] for row in view["term_table"]] == ["Job", "Stage", "Task"]
    assert all(row["Trong ví dụ"] for row in view["term_table"])
    assert all(isinstance(row["Phần dữ liệu ban đầu"], str) for row in view["input_table"])
    facts = {row["Ghi nhận"]: row["Số lượng"] for row in view["result_table"]}
    assert facts["Job"] == 4
    assert facts["Stage"] == 8
    assert facts["Task được tạo"] == 20
    assert facts["Task hoàn tất ghi nhận"] == 7
    assert facts["Task lỗi"] == 0
    assert "4 Job" in view["reading_hint"]
    assert "Task chưa hoàn tất không tự động là lỗi" in view["reading_hint"]


def test_execution_view_does_not_turn_missing_tracker_into_zero() -> None:
    view = build_step_view({"id": "execution", "result": {}})

    assert all(row["Số lượng"] == "Chưa có số liệu" for row in view["result_table"])


def test_tables_use_clear_time_labels_and_keep_lazy_duration_separate() -> None:
    status = load_demo_status()
    views = {step["id"]: build_step_view(step) for step in status["steps"]}

    parquet = views["parquet"]
    assert "Thời gian chạy" in parquet["result_table"][0]
    assert all(isinstance(row["Thời gian chạy"], str) for row in parquet["result_table"])
    assert "nhiều lần đo" in parquet["reading_hint"]

    cache = views["cache"]
    assert "Thời gian chạy" in cache["result_table"][0]
    assert "nạp lần đầu" in cache["reading_hint"].lower()

    lazy = views["lazy"]
    assert "Thời gian action" in lazy["result_table"][0]
    action_row = next(row for row in lazy["result_table"] if row["Thời điểm"] == "Thời gian action")
    assert action_row["Số Job"] == "—"
    assert action_row["Thời gian action"].endswith("giây")


def test_execution_copy_uses_counts_from_the_selected_run() -> None:
    step = {
        "id": "execution",
        "result": {
            "shuffle_exchange_nodes": 3,
            "status_tracker": {
                "job_count": 2,
                "unique_stage_count": 5,
                "total_tasks_across_unique_stages": 10,
                "completed_tasks_across_unique_stages": 8,
                "failed_tasks_across_unique_stages": 1,
            },
        },
    }

    view = build_step_view(step)

    assert "2 Job" in view["reading_hint"]
    assert "5 Stage" in view["reading_hint"]
    assert "10 Task" in view["reading_hint"]
    assert "20 Task" not in view["reading_hint"]
    facts = {row["Ghi nhận"]: row["Số lượng"] for row in view["result_table"]}
    assert facts["Số Exchange trong kế hoạch"] == 3


def test_execution_copy_explains_missing_counts_without_inventing_values() -> None:
    view = build_step_view({"id": "execution", "result": {}})

    assert "20 Task" not in view["reading_hint"]
    assert "7" not in view["reading_hint"]
    assert "0 Task" not in view["reading_hint"]
    assert "Job" in view["reading_hint"] and "Stage" in view["reading_hint"] and "Task" in view["reading_hint"]


def test_cache_conclusion_follows_the_measured_timings() -> None:
    slower = build_step_view(
        {
            "id": "cache",
            "result": {
                "without_cache": {"median_seconds": 0.279},
                "with_cache": {"median_seconds": 0.347},
                "cache_materialization_seconds": 1.745,
            },
        }
    )
    faster = build_step_view(
        {
            "id": "cache",
            "result": {
                "without_cache": {"median_seconds": 0.8},
                "with_cache": {"median_seconds": 0.2},
                "cache_materialization_seconds": 1.1,
            },
        }
    )
    missing = build_step_view({"id": "cache", "result": {}})

    assert "chậm hơn" in slower["conclusion"]
    assert "0.347 giây" in slower["conclusion"]
    assert "1.745 giây" in slower["conclusion"]
    assert "nhanh hơn" in faster["conclusion"]
    assert "Chưa đủ số liệu" in missing["conclusion"]


def test_mllib_and_streaming_explain_their_model_relationship() -> None:
    status = load_demo_status()
    views = {step["id"]: build_step_view(step) for step in status["steps"]}

    assert "mô hình mẫu" in views["mllib"]["reading_hint"]
    assert "chưa được lưu" in views["mllib"]["ids_link"]
    assert "mô hình đã lưu" in views["streaming"]["ids_link"]
    assert "đợt xử lý nhỏ" in views["streaming"]["reading_hint"]
    assert "dấu mốc" in views["streaming"]["conclusion"]
    assert views["mllib"]["input_table"][0]["Tập dữ liệu"] == "Dữ liệu học (Train)"
    assert "Đợt xử lý (batch)" in views["streaming"]["result_table"][0]


def test_ids_steps_name_sample_and_file_simulation() -> None:
    mllib = build_step_view({"id": "mllib", "result": {}})
    streaming = build_step_view({"id": "streaming", "result": {}})

    assert "mẫu" in mllib["reading_hint"].lower()
    assert "kiểm thử cuối" in mllib["reading_hint"].lower()
    assert "mô phỏng" in streaming["reading_hint"].lower()
    assert "bắt gói tin" in streaming["reading_hint"].lower()
    assert all(row["Số flow"] == "Chưa có số liệu" for row in mllib["result_table"])
    assert streaming["result_table"][0]["Đợt xử lý (batch)"] == "Chưa có số liệu"


def test_execution_step_shows_what_job_stage_task_do() -> None:
    app = AppTest.from_file(str(PROJECT_ROOT / "dashboard" / "app.py"), default_timeout=30)
    app.session_state.demo_step = 3
    app.run()

    assert not app.exception
    text = "\n".join(item.value for item in app.markdown)
    assert "JOB / STAGE / TASK LÀM GÌ?" in text
    tables = [item.value for item in app.dataframe]
    assert any(list(table["Spark gọi"]) == ["Job", "Stage", "Task"] for table in tables if "Spark gọi" in table.columns)


def test_execution_step_shows_a_conceptual_diagram_before_measured_counts() -> None:
    app = AppTest.from_file(str(PROJECT_ROOT / "dashboard" / "app.py"), default_timeout=30)
    app.session_state.demo_step = 3
    app.run()

    assert not app.exception
    page_text = "\n".join(item.value for item in app.markdown)
    assert "SƠ ĐỒ NGUYÊN LÝ" in page_text
    assert "1 Job" in page_text
    assert "nhiều Stage" in page_text
    assert "mỗi Stage có nhiều Task" in page_text
    assert "SỐ LIỆU CỦA LẦN CHẠY NÀY" in page_text


def test_execution_diagram_has_a_compact_responsive_style() -> None:
    source = (PROJECT_ROOT / "dashboard" / "styles.py").read_text(encoding="utf-8")

    for selector in (".execution-diagram", ".diagram-node", ".diagram-arrow"):
        assert selector in source

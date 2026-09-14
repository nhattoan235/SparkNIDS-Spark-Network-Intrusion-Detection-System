"""Phase-11 documentation regression checks against generated source artifacts."""

from __future__ import annotations

import json

from src.spark_session import PROJECT_ROOT


def test_final_report_and_demo_reference_authoritative_metrics() -> None:
    report = (PROJECT_ROOT / "docs" / "BAO_CAO_KY_THUAT.md").read_text(encoding="utf-8")
    demo = (PROJECT_ROOT / "docs" / "KICH_BAN_DEMO.md").read_text(encoding="utf-8")
    final_metrics = json.loads(
        (PROJECT_ROOT / "outputs" / "metrics" / "phase7_final_test.json").read_text(encoding="utf-8")
    )
    metrics = final_metrics["evaluation"]["threshold_metrics"]

    normalized_report = report.replace(",", ".")
    assert f"{metrics['recall']:.6f}" in normalized_report
    assert f"{metrics['false_positive_rate']:.6f}" in normalized_report
    assert "phase7_final_test.json" in report
    assert "phase10_streaming.json" in report
    assert "Không chạy lại official test" in demo


def test_phase11_documents_include_runbook_and_limitations() -> None:
    report = (PROJECT_ROOT / "docs" / "BAO_CAO_KY_THUAT.md").read_text(encoding="utf-8")
    demo = (PROJECT_ROOT / "docs" / "KICH_BAN_DEMO.md").read_text(encoding="utf-8")
    guide = (PROJECT_ROOT / "docs" / "DASHBOARD_GUIDE.md").read_text(encoding="utf-8")
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    for needle in ("## 10. Hạn chế", "## 11. Khả năng tái lập", "PipelineModel", "Structured Streaming"):
        assert needle in report
    for needle in ("## Phương án dự phòng", "## Checklist nghiệm thu", "0:00–0:45"):
        assert needle in demo
    for needle in ("src.spark_demo", "127.0.0.1:4040", "127.0.0.1:8501", "Bảy bước"):
        assert needle in guide
    for needle in ("guided demo", "src.spark_demo", "Structured Streaming"):
        assert needle in readme

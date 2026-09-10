"""Phase-3 Spark SQL EDA and bounded visualization outputs."""

from __future__ import annotations

import argparse
import html
import json
import math
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.unsw_nb15_schema import NUMERIC_COLUMNS


def _to_json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: _to_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_json_value(item) for item in value]
    return value


def _collect_small(dataframe: DataFrame) -> list[dict[str, Any]]:
    return [_to_json_value(row.asDict(recursive=True)) for row in dataframe.collect()]


def run_sql_queries(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    top_n: int,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    """Run named Spark SQL aggregates; every result is deliberately bounded."""
    dataframe.createOrReplaceTempView("traffic")
    queries = {
        "class_distribution": """
            WITH class_counts AS (
                SELECT CASE WHEN label = 1 THEN 'Attack' ELSE 'Normal' END AS traffic_class,
                       COUNT(*) AS flow_count
                FROM traffic
                GROUP BY label
            ), totals AS (SELECT SUM(flow_count) AS total_count FROM class_counts)
            SELECT traffic_class, flow_count,
                   ROUND(100.0 * flow_count / total_count, 4) AS percentage
            FROM class_counts CROSS JOIN totals
            ORDER BY flow_count DESC
        """,
        "attack_category_distribution": f"""
            WITH category_counts AS (
                SELECT attack_cat, COUNT(*) AS flow_count
                FROM traffic
                GROUP BY attack_cat
            ), totals AS (SELECT SUM(flow_count) AS total_count FROM category_counts)
            SELECT attack_cat, flow_count,
                   ROUND(100.0 * flow_count / total_count, 4) AS percentage
            FROM category_counts CROSS JOIN totals
            ORDER BY flow_count DESC
            LIMIT {top_n}
        """,
        "protocol_attack_rate": f"""
            SELECT proto, COUNT(*) AS flow_count, SUM(label) AS attack_count,
                   ROUND(100.0 * SUM(label) / COUNT(*), 4) AS attack_rate_percent
            FROM traffic
            GROUP BY proto
            ORDER BY flow_count DESC
            LIMIT {top_n}
        """,
        "service_attack_rate": f"""
            SELECT service, COUNT(*) AS flow_count, SUM(label) AS attack_count,
                   ROUND(100.0 * SUM(label) / COUNT(*), 4) AS attack_rate_percent
            FROM traffic
            GROUP BY service
            ORDER BY flow_count DESC
            LIMIT {top_n}
        """,
        "state_attack_rate": f"""
            SELECT state, COUNT(*) AS flow_count, SUM(label) AS attack_count,
                   ROUND(100.0 * SUM(label) / COUNT(*), 4) AS attack_rate_percent
            FROM traffic
            GROUP BY state
            ORDER BY flow_count DESC
            LIMIT {top_n}
        """,
        "numeric_profile_by_class": """
            SELECT CASE WHEN label = 1 THEN 'Attack' ELSE 'Normal' END AS traffic_class,
                   ROUND(AVG(dur), 6) AS avg_duration,
                   ROUND(PERCENTILE_APPROX(dur, 0.5), 6) AS median_duration,
                   ROUND(AVG(sbytes), 2) AS avg_source_bytes,
                   ROUND(AVG(dbytes), 2) AS avg_destination_bytes,
                   ROUND(AVG(rate), 2) AS avg_rate,
                   ROUND(AVG(spkts), 2) AS avg_source_packets,
                   ROUND(AVG(dpkts), 2) AS avg_destination_packets
            FROM traffic
            GROUP BY label
            ORDER BY traffic_class
        """,
    }
    return ({name: _collect_small(spark.sql(query)) for name, query in queries.items()}, queries)


def compute_numeric_correlations(dataframe: DataFrame, top_n: int) -> list[dict[str, Any]]:
    expressions = [F.corr(F.col(column).cast("double"), F.col("label")).alias(column) for column in NUMERIC_COLUMNS]
    values = dataframe.agg(*expressions).first().asDict()
    results = [
        {"feature": feature, "correlation_with_label": float(value)}
        for feature, value in values.items()
        if value is not None and math.isfinite(float(value))
    ]
    return sorted(results, key=lambda item: abs(item["correlation_with_label"]), reverse=True)[:top_n]


def write_bar_chart(
    destination: Path,
    *,
    title: str,
    items: Iterable[tuple[str, float]],
    value_label: str,
) -> None:
    values = list(items)
    width = 960
    left = 245
    right = 90
    top = 70
    row_height = 34
    height = top + max(1, len(values)) * row_height + 45
    maximum = max((abs(value) for _, value in values), default=1.0) or 1.0
    usable_width = width - left - right
    rows: list[str] = []
    for index, (label, value) in enumerate(values):
        y = top + index * row_height
        bar_width = abs(value) / maximum * usable_width
        rows.extend(
            [
                f'<text x="{left - 10}" y="{y + 18}" text-anchor="end">{html.escape(label)}</text>',
                f'<rect x="{left}" y="{y + 3}" width="{bar_width:.2f}" height="21" rx="3" />',
                f'<text x="{left + bar_width + 8:.2f}" y="{y + 18}">{value:,.4g}</text>',
            ]
        )
    svg = "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            "<style>text{font:14px Segoe UI,Arial,sans-serif;fill:#1f2937} rect{fill:#2563eb} .title{font-size:22px;font-weight:600}.axis{font-size:12px;fill:#6b7280}</style>",
            '<rect width="100%" height="100%" fill="#ffffff"/>',
            f'<text class="title" x="24" y="36">{html.escape(title)}</text>',
            f'<text class="axis" x="{left}" y="{height - 14}">{html.escape(value_label)}</text>',
            *rows,
            "</svg>",
        ]
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(svg, encoding="utf-8")


def _markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> list[str]:
    lines = ["| " + " | ".join(columns) + " |", "|" + "|".join("---" for _ in columns) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    return lines


def render_eda_markdown(report: dict[str, Any]) -> str:
    sql = report["sql_results"]
    imbalance = report["class_imbalance"]
    lines = [
        "# EDA UNSW-NB15 bằng Spark SQL",
        "",
        f"Sinh lúc: `{report['generated_at']}` từ Silver train `{report['training_row_count']}` dòng.",
        "",
        "## Phân phối lớp",
        "",
        *_markdown_table(sql["class_distribution"], ["traffic_class", "flow_count", "percentage"]),
        "",
        f"Tỷ lệ lớp lớn/lớp nhỏ là **{imbalance['majority_to_minority_ratio']:.4f}:1**. "
        "Attack chiếm đa số trong train; vì chi phí bỏ sót tấn công cao, các phase mô hình vẫn phải "
        "ưu tiên Recall/PR-AUC đồng thời theo dõi False Positive Rate thay vì dựa vào Accuracy.",
        "",
        "![Phân phối lớp](../outputs/figures/class_distribution.svg)",
        "",
        "## Nhóm tấn công",
        "",
        *_markdown_table(
            sql["attack_category_distribution"], ["attack_cat", "flow_count", "percentage"]
        ),
        "",
        "![Nhóm tấn công](../outputs/figures/attack_category_distribution.svg)",
        "",
        "## Protocol phổ biến và attack rate",
        "",
        *_markdown_table(
            sql["protocol_attack_rate"],
            ["proto", "flow_count", "attack_count", "attack_rate_percent"],
        ),
        "",
        "![Protocol phổ biến](../outputs/figures/protocol_flow_count.svg)",
        "",
        "## Service phổ biến và attack rate",
        "",
        *_markdown_table(
            sql["service_attack_rate"],
            ["service", "flow_count", "attack_count", "attack_rate_percent"],
        ),
        "",
        "## Trạng thái kết nối và attack rate",
        "",
        *_markdown_table(
            sql["state_attack_rate"],
            ["state", "flow_count", "attack_count", "attack_rate_percent"],
        ),
        "",
        "## Hồ sơ đặc trưng số theo lớp",
        "",
        *_markdown_table(
            sql["numeric_profile_by_class"],
            [
                "traffic_class",
                "avg_duration",
                "median_duration",
                "avg_source_bytes",
                "avg_destination_bytes",
                "avg_rate",
                "avg_source_packets",
                "avg_destination_packets",
            ],
        ),
        "",
        "## Tương quan số với label",
        "",
        *_markdown_table(report["top_numeric_correlations"], ["feature", "correlation_with_label"]),
        "",
        "![Tương quan số](../outputs/figures/numeric_label_correlation.svg)",
        "",
        "## Bằng chứng Spark SQL",
        "",
        f"Pipeline đã chạy **{report['sql_query_count']}** truy vấn Spark SQL có tên: "
        + ", ".join(f"`{name}`" for name in report["sql_query_names"])
        + ". Kết quả đầy đủ cho service, state và numeric profile nằm trong "
        "`outputs/metrics/phase3_eda.json`.",
        "",
        "Không có thao tác `toPandas()` hoặc collect toàn bộ dữ liệu; chỉ các bảng aggregate đã giới hạn được đưa về driver.",
        "",
    ]
    return "\n".join(lines)


def run_eda(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    spark = create_spark_session(config, app_name="unsw-nb15-phase3-eda")
    silver_train = PROJECT_ROOT / config["paths"]["silver"] / "train"
    output_root = PROJECT_ROOT / config["paths"]["outputs"]
    figures_root = output_root / "figures"

    try:
        dataframe = spark.read.parquet(str(silver_train)).cache()
        training_row_count = dataframe.count()
        sql_results, queries = run_sql_queries(
            spark,
            dataframe,
            top_n=int(config["eda"]["top_n"]),
        )
        correlations = compute_numeric_correlations(
            dataframe,
            top_n=int(config["eda"]["correlation_top_n"]),
        )
        class_counts = [int(row["flow_count"]) for row in sql_results["class_distribution"]]
        majority = max(class_counts)
        minority = min(class_counts)
        report = {
            "phase": 3,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "spark_version": spark.version,
            "dataset": "silver/train",
            "training_row_count": training_row_count,
            "collection_policy": "Only bounded aggregate results are collected; no toPandas().",
            "sql_query_count": len(queries),
            "sql_query_names": list(queries),
            "sql_queries": {name: " ".join(query.split()) for name, query in queries.items()},
            "sql_results": sql_results,
            "top_numeric_correlations": correlations,
            "class_imbalance": {
                "majority_count": majority,
                "minority_count": minority,
                "majority_to_minority_ratio": majority / minority,
            },
        }
        dataframe.unpersist()
    finally:
        spark.stop()

    write_bar_chart(
        figures_root / "class_distribution.svg",
        title="UNSW-NB15 training class distribution",
        items=[(row["traffic_class"], float(row["flow_count"])) for row in sql_results["class_distribution"]],
        value_label="Flow count",
    )
    write_bar_chart(
        figures_root / "attack_category_distribution.svg",
        title="UNSW-NB15 training attack categories",
        items=[(row["attack_cat"], float(row["flow_count"])) for row in sql_results["attack_category_distribution"]],
        value_label="Flow count",
    )
    write_bar_chart(
        figures_root / "protocol_flow_count.svg",
        title="Top protocols by flow count",
        items=[(row["proto"], float(row["flow_count"])) for row in sql_results["protocol_attack_rate"]],
        value_label="Flow count",
    )
    write_bar_chart(
        figures_root / "numeric_label_correlation.svg",
        title="Top absolute numeric correlations with label",
        items=[
            (item["feature"], abs(float(item["correlation_with_label"])))
            for item in correlations
        ],
        value_label="Absolute Pearson correlation",
    )

    metrics_path = output_root / "metrics" / "phase3_eda.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    dashboard_path = output_root / "dashboard" / "eda_summary.json"
    dashboard_path.parent.mkdir(parents=True, exist_ok=True)
    dashboard_path.write_text(json.dumps(report["sql_results"], indent=2, ensure_ascii=False), encoding="utf-8")
    document_path = PROJECT_ROOT / "docs" / "EDA_REPORT.md"
    document_path.write_text(render_eda_markdown(report), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run bounded Spark SQL EDA on Silver train data.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_eda(arguments.config)

"""Phase-2 ingestion: validate CSVs, write Bronze Parquet, and create a sample."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.unsw_nb15_schema import EXPECTED_COLUMNS
from src.validate_data import read_unsw_csv, validate_dataframe


def _write_sample(dataframe: DataFrame, destination: Path, rows_per_label: int) -> int:
    stable_hash = F.xxhash64(*[F.col(column) for column in EXPECTED_COLUMNS])
    window = Window.partitionBy("label").orderBy(stable_hash, F.col("id"))
    sample = (
        dataframe.withColumn("_sample_rank", F.row_number().over(window))
        .filter(F.col("_sample_rank") <= rows_per_label)
        .drop("_sample_rank")
        .orderBy("label", "id")
    )
    sample.coalesce(1).write.mode("overwrite").option("header", True).csv(str(destination))
    return sample.count()


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Báo cáo validation dữ liệu UNSW-NB15",
        "",
        f"Sinh lúc: `{report['generated_at']}`",
        "",
        f"Nguồn chính thức: {report['official_page']}",
        "",
        "| Split | Rows | Columns | Duplicates | Invalid labels | SHA-256 | Bronze read-back |",
        "|---|---:|---:|---:|---:|---|---:|",
    ]
    for split_report in report["splits"]:
        lines.append(
            "| {split} | {rows} | {columns} | {duplicates} | {invalid} | {sha} | {bronze} |".format(
                split=split_report["split"],
                rows=split_report["row_count"],
                columns=split_report["column_count"],
                duplicates=split_report["duplicate_row_count"],
                invalid=split_report["invalid_label_count"],
                sha="khớp" if split_report["sha256_matches"] else "không khớp",
                bronze=split_report["bronze_row_count"],
            )
        )

    lines.extend(["", "## Phân phối nhãn", ""])
    for split_report in report["splits"]:
        distribution = ", ".join(
            f"label={item['label']}: {item['count']}" for item in split_report["label_distribution"]
        )
        lines.append(f"- **{split_report['split']}**: {distribution}")

    lines.extend(
        [
            "",
            "## Ghi chú",
            "",
            "- CSV được đọc bằng `StructType` tường minh, không dùng `inferSchema`.",
            "- Bronze giữ nguyên 45 cột của designated train/test split và được ghi Snappy Parquet.",
            "- Chi tiết schema, null từng cột và attack category nằm trong `outputs/metrics/phase2_validation.json`.",
            f"- Sample cố định có {report['sample_row_count']} dòng, cân bằng theo nhãn và chỉ dùng cho test.",
            "",
        ]
    )
    return "\n".join(lines)


def run_ingestion(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    spark = create_spark_session(config, app_name="unsw-nb15-phase2-ingest")
    raw_dir = PROJECT_ROOT / config["paths"]["raw"]
    bronze_dir = PROJECT_ROOT / config["paths"]["bronze"]
    sample_dir = PROJECT_ROOT / config["paths"]["samples"] / "unsw_nb15_sample_csv"
    split_reports: list[dict[str, Any]] = []

    try:
        training_dataframe: DataFrame | None = None
        for split, file_config in config["dataset"]["files"].items():
            source_path = raw_dir / file_config["filename"]
            dataframe = read_unsw_csv(spark, source_path)
            split_report = validate_dataframe(
                dataframe,
                path=source_path,
                split=split,
                expected_rows=int(file_config["expected_rows"]),
                expected_sha256=file_config["sha256"],
            )
            if not split_report["valid"]:
                raise ValueError(f"Dataset validation failed for split: {split}")

            bronze_path = bronze_dir / split
            dataframe.write.mode("overwrite").option("compression", "snappy").parquet(str(bronze_path))
            bronze_dataframe = spark.read.parquet(str(bronze_path))
            split_report["bronze_path"] = str(bronze_path)
            split_report["bronze_row_count"] = bronze_dataframe.count()
            split_report["bronze_schema_matches"] = bronze_dataframe.schema == dataframe.schema
            if split_report["bronze_row_count"] != split_report["row_count"]:
                raise ValueError(f"Bronze row-count mismatch for split: {split}")
            split_reports.append(split_report)
            if split == "train":
                training_dataframe = dataframe

        if training_dataframe is None:
            raise ValueError("Training split is required to create the fixed sample.")
        sample_row_count = _write_sample(
            training_dataframe,
            sample_dir,
            int(config["dataset"]["sample_rows_per_label"]),
        )

        report = {
            "phase": 2,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "official_page": config["dataset"]["official_page"],
            "spark_version": spark.version,
            "splits": split_reports,
            "sample_path": str(sample_dir),
            "sample_row_count": sample_row_count,
        }
    finally:
        spark.stop()

    metrics_path = PROJECT_ROOT / config["paths"]["outputs"] / "metrics" / "phase2_validation.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    markdown_path = PROJECT_ROOT / "docs" / "DATA_VALIDATION.md"
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate UNSW-NB15 and write Bronze Parquet.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_ingestion(arguments.config)


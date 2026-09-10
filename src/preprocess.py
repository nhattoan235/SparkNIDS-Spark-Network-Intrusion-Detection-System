"""Phase-3 cleaning from validated Bronze Parquet to reusable Silver Parquet."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.unsw_nb15_schema import (
    CATEGORICAL_COLUMNS,
    EXPECTED_COLUMNS,
    NUMERIC_COLUMNS,
    UNSW_NB15_SCHEMA,
)


CANONICAL_ATTACK_CATEGORIES = {
    "analysis": "Analysis",
    "backdoor": "Backdoor",
    "backdoors": "Backdoor",
    "dos": "DoS",
    "exploits": "Exploits",
    "fuzzers": "Fuzzers",
    "generic": "Generic",
    "reconnaissance": "Reconnaissance",
    "shellcode": "Shellcode",
    "worms": "Worms",
}


def _is_missing_category(column: str) -> Column:
    trimmed = F.trim(F.col(column))
    return F.col(column).isNull() | (trimmed == "") | (trimmed == "-")


def _is_invalid_numeric(column: str) -> Column:
    field = UNSW_NB15_SCHEMA[column]
    value = F.col(column)
    condition = value.isNull() | (value < 0)
    if isinstance(field.dataType, DoubleType):
        condition = condition | F.isnan(value) | value.isin(float("inf"), float("-inf"))
    return condition


def _is_invalid_target() -> Column:
    normalized_attack = F.lower(F.trim(F.col("attack_cat")))
    missing_attack = F.col("attack_cat").isNull() | normalized_attack.isin("", "-")
    mismatch = missing_attack | ((F.col("label") == 0) & (normalized_attack != "normal")) | (
        (F.col("label") == 1) & (missing_attack | (normalized_attack == "normal"))
    )
    return (
        F.col("id").isNull()
        | (F.col("id") <= 0)
        | F.col("label").isNull()
        | ~F.col("label").isin(0, 1)
        | mismatch
    )


def audit_dataframe(dataframe: DataFrame) -> dict[str, Any]:
    """Return bounded aggregate diagnostics without collecting source rows."""
    cached = dataframe.cache()
    try:
        row_count = cached.count()
        aggregate_expressions: list[Column] = []
        for column in NUMERIC_COLUMNS:
            aggregate_expressions.append(
                F.sum(F.when(_is_invalid_numeric(column), 1).otherwise(0)).alias(f"numeric__{column}")
            )
        for column in CATEGORICAL_COLUMNS:
            aggregate_expressions.append(
                F.sum(F.when(_is_missing_category(column), 1).otherwise(0)).alias(
                    f"categorical__{column}"
                )
            )
        aggregate_expressions.append(
            F.sum(F.when(_is_invalid_target(), 1).otherwise(0)).alias("invalid_target_rows")
        )
        aggregate = cached.agg(*aggregate_expressions).first().asDict()
        duplicate_count = row_count - cached.dropDuplicates(EXPECTED_COLUMNS).count()
        return {
            "row_count": row_count,
            "duplicate_row_count": duplicate_count,
            "invalid_target_row_count": int(aggregate.pop("invalid_target_rows")),
            "invalid_numeric_counts": {
                key.removeprefix("numeric__"): int(value)
                for key, value in aggregate.items()
                if key.startswith("numeric__")
            },
            "missing_categorical_counts": {
                key.removeprefix("categorical__"): int(value)
                for key, value in aggregate.items()
                if key.startswith("categorical__")
            },
        }
    finally:
        cached.unpersist()


def clean_dataframe(dataframe: DataFrame, *, missing_token: str = "__unknown__") -> DataFrame:
    """Apply deterministic cleaning while preserving the 45-column contract."""
    cleaned = dataframe.filter(~_is_invalid_target()).dropDuplicates(EXPECTED_COLUMNS)

    for column in CATEGORICAL_COLUMNS:
        normalized = F.lower(F.trim(F.col(column)))
        cleaned = cleaned.withColumn(
            column,
            F.when(_is_missing_category(column), F.lit(missing_token)).otherwise(normalized),
        )

    attack_mapping = F.create_map(
        *[
            item
            for key, value in CANONICAL_ATTACK_CATEGORIES.items()
            for item in (F.lit(key), F.lit(value))
        ]
    )
    normalized_attack = F.lower(F.trim(F.col("attack_cat")))
    cleaned = cleaned.withColumn(
        "attack_cat",
        F.when(F.col("label") == 0, F.lit("Normal")).otherwise(
            F.coalesce(F.element_at(attack_mapping, normalized_attack), F.trim(F.col("attack_cat")))
        ),
    )

    for column in NUMERIC_COLUMNS:
        data_type = UNSW_NB15_SCHEMA[column].dataType
        cleaned = cleaned.withColumn(
            column,
            F.when(_is_invalid_numeric(column), F.lit(0).cast(data_type))
            .otherwise(F.col(column))
            .cast(data_type),
        )

    return cleaned.select(*EXPECTED_COLUMNS)


def _audit_is_clean(audit: dict[str, Any]) -> bool:
    return (
        audit["duplicate_row_count"] == 0
        and audit["invalid_target_row_count"] == 0
        and all(value == 0 for value in audit["invalid_numeric_counts"].values())
        and all(value == 0 for value in audit["missing_categorical_counts"].values())
    )


def run_preprocessing(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    spark = create_spark_session(config, app_name="unsw-nb15-phase3-clean")
    bronze_root = PROJECT_ROOT / config["paths"]["bronze"]
    silver_root = PROJECT_ROOT / config["paths"]["silver"]
    split_reports: list[dict[str, Any]] = []

    try:
        for split in config["dataset"]["files"]:
            source_path = bronze_root / split
            destination_path = silver_root / split
            bronze = spark.read.parquet(str(source_path))
            before = audit_dataframe(bronze)
            silver = clean_dataframe(
                bronze,
                missing_token=config["cleaning"]["categorical_missing_token"],
            )
            after = audit_dataframe(silver)
            silver.write.mode("overwrite").option("compression", "snappy").parquet(
                str(destination_path)
            )
            reloaded = spark.read.parquet(str(destination_path))
            reloaded_count = reloaded.count()
            schema_matches = [field.dataType for field in reloaded.schema.fields] == [
                field.dataType for field in UNSW_NB15_SCHEMA.fields
            ]
            if reloaded_count != after["row_count"] or not schema_matches or not _audit_is_clean(after):
                raise ValueError(f"Silver validation failed for split: {split}")
            split_reports.append(
                {
                    "split": split,
                    "before": before,
                    "after": after,
                    "dropped_row_count": before["row_count"] - after["row_count"],
                    "silver_path": str(destination_path),
                    "silver_row_count": reloaded_count,
                    "silver_schema_matches": schema_matches,
                }
            )
        report = {
            "phase": 3,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "spark_version": spark.version,
            "cleaning_rules": {
                "categorical_missing_token": config["cleaning"]["categorical_missing_token"],
                "numeric_invalid_replacement": 0,
                "numeric_features_must_be_nonnegative": True,
                "invalid_target_rows": "drop",
                "duplicate_rows": "drop",
            },
            "splits": split_reports,
        }
    finally:
        spark.stop()

    metrics_path = PROJECT_ROOT / config["paths"]["outputs"] / "metrics" / "phase3_cleaning.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean Bronze UNSW-NB15 into Silver Parquet.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_preprocessing(arguments.config)

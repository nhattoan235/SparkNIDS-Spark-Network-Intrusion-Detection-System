"""Schema-enforced loading and validation for UNSW-NB15 CSV files."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.download_data import sha256_file
from src.unsw_nb15_schema import EXPECTED_COLUMNS, UNSW_NB15_SCHEMA


def read_header(path: Path) -> tuple[str, ...]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return tuple(next(csv.reader(stream)))


def validate_header(path: Path, expected: Sequence[str] = EXPECTED_COLUMNS) -> None:
    actual = read_header(path)
    if tuple(expected) != actual:
        raise ValueError(
            f"Unexpected CSV header in {path}. Expected {len(expected)} columns, "
            f"received {len(actual)}."
        )


def read_unsw_csv(spark: SparkSession, path: Path) -> DataFrame:
    validate_header(path)
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .option("enforceSchema", True)
        .schema(UNSW_NB15_SCHEMA)
        .csv(str(path))
    )


def _count_nulls(dataframe: DataFrame) -> dict[str, int]:
    expressions = [
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in dataframe.columns
    ]
    return {key: int(value) for key, value in dataframe.agg(*expressions).first().asDict().items()}


def _distribution(dataframe: DataFrame, column: str) -> list[dict[str, Any]]:
    return [
        row.asDict(recursive=True)
        for row in dataframe.groupBy(column).count().orderBy(F.desc("count"), column).collect()
    ]


def validate_dataframe(
    dataframe: DataFrame,
    *,
    path: Path,
    split: str,
    expected_rows: int,
    expected_sha256: str,
) -> dict[str, Any]:
    cached = dataframe.cache()
    try:
        row_count = cached.count()
        distinct_rows = cached.dropDuplicates().count()
        invalid_label_count = cached.filter(~F.col("label").isin(0, 1) | F.col("label").isNull()).count()
        actual_sha256 = sha256_file(path)
        report = {
            "split": split,
            "source_file": str(path),
            "source_bytes": path.stat().st_size,
            "sha256": actual_sha256,
            "sha256_matches": actual_sha256 == expected_sha256,
            "row_count": row_count,
            "expected_rows": expected_rows,
            "row_count_matches": row_count == expected_rows,
            "column_count": len(cached.columns),
            "columns": cached.columns,
            "schema": [
                {"name": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable}
                for field in cached.schema.fields
            ],
            "null_counts": _count_nulls(cached),
            "duplicate_row_count": row_count - distinct_rows,
            "invalid_label_count": invalid_label_count,
            "label_distribution": _distribution(cached, "label"),
            "attack_category_distribution": _distribution(cached, "attack_cat"),
        }
        report["valid"] = all(
            (
                report["sha256_matches"],
                report["row_count_matches"],
                report["column_count"] == len(EXPECTED_COLUMNS),
                report["invalid_label_count"] == 0,
            )
        )
        return report
    finally:
        cached.unpersist()


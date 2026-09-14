"""Small Spark experiments used by the guided presentation dashboard."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.benchmark import _format_benchmark, representative_query


def summarize_data(dataframe: DataFrame) -> dict[str, Any]:
    """Collect only bounded metadata and label aggregates for the first demo step."""
    label_distribution = [
        {"label": int(row["label"]), "count": int(row["count"])}
        for row in dataframe.groupBy("label").count().orderBy("label").collect()
    ]
    return {
        "row_count": int(dataframe.count()),
        "column_count": len(dataframe.columns),
        "columns": list(dataframe.columns),
        "schema": [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": bool(field.nullable),
            }
            for field in dataframe.schema.fields
        ],
        "label_distribution": label_distribution,
    }


def run_format_comparison(
    spark: SparkSession,
    csv_path: str | Path,
    parquet_path: str | Path,
    *,
    warmups: int = 1,
    runs: int = 2,
) -> dict[str, Any]:
    """Run the existing controlled benchmark and shape it for one guided step."""
    benchmark = _format_benchmark(
        spark,
        csv_path=Path(csv_path),
        parquet_path=Path(parquet_path),
        warmups=int(warmups),
        runs=int(runs),
    )
    csv_median = float(benchmark["csv"]["timing"]["median_seconds"])
    parquet_median = float(benchmark["parquet"]["timing"]["median_seconds"])
    return {
        "flow": {
            "input": "Cùng dữ liệu UNSW-NB15 ở CSV và Parquet",
            "spark": "Đọc → filter label → groupBy proto → aggregate → collect",
            "result": "Cùng aggregate, khác chi phí đọc",
        },
        "metrics": [
            {"label": "CSV median", "value": f"{csv_median:.3f}s"},
            {"label": "Parquet median", "value": f"{parquet_median:.3f}s"},
            {"label": "Parquet speedup", "value": f"{benchmark['parquet_speedup_over_csv_median']:.2f}x"},
        ],
        "explanation": (
            "Spark chạy cùng một truy vấn trên hai format. Parquet thường đọc nhanh hơn "
            "vì schema/cột đã được lưu theo dạng máy đọc được; số đo này chỉ đúng cho lần chạy hiện tại."
        ),
        "evidence": {
            "identical_results": bool(benchmark["identical_results"]),
            "csv_result_sha256": benchmark["csv"]["result_sha256"],
            "parquet_result_sha256": benchmark["parquet"]["result_sha256"],
            "csv_size_bytes": int(benchmark["csv"]["size_bytes"]),
            "parquet_size_bytes": int(benchmark["parquet"]["size_bytes"]),
        },
        "identical_results": bool(benchmark["identical_results"]),
        "benchmark": benchmark,
    }


def build_representative_query(dataframe: DataFrame) -> DataFrame:
    """Expose the same query in a named teaching-oriented API."""
    return representative_query(dataframe)


def count_exchange_nodes(dataframe: DataFrame) -> int:
    """Count Exchange nodes in Spark's currently reported physical plan."""
    return dataframe._jdf.queryExecution().executedPlan().toString().count("Exchange")

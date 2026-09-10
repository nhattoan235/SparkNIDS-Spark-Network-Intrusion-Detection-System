"""Leakage-safe dataset split and Spark ML feature/model pipeline for Phase 4."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.spark_session import PROJECT_ROOT, create_spark_session, load_config
from src.unsw_nb15_schema import CATEGORICAL_COLUMNS, NUMERIC_COLUMNS


def split_train_validation(
    dataframe: DataFrame,
    *,
    validation_fraction: float,
    seed: int,
    hash_buckets: int = 10_000,
) -> tuple[DataFrame, DataFrame]:
    """Split deterministically without touching the designated test set."""
    if not 0.0 < validation_fraction < 1.0:
        raise ValueError("validation_fraction must be strictly between 0 and 1")
    if hash_buckets < 100:
        raise ValueError("hash_buckets must be at least 100")

    validation_buckets = int(round(validation_fraction * hash_buckets))
    bucket = F.pmod(
        F.xxhash64(F.col("id"), F.lit(int(seed))),
        F.lit(hash_buckets),
    )
    assigned = dataframe.withColumn("_split_bucket", bucket)
    training = assigned.filter(F.col("_split_bucket") >= validation_buckets).drop("_split_bucket")
    validation = assigned.filter(F.col("_split_bucket") < validation_buckets).drop("_split_bucket")
    return training, validation


def build_feature_pipeline(config: dict[str, Any] | None = None) -> Pipeline:
    """Build fitted-on-train-only feature stages ending in the `features` vector."""
    resolved = config or load_config()
    modeling = resolved["modeling"]
    index_columns = [f"{column}_index" for column in CATEGORICAL_COLUMNS]
    encoded_columns = [f"{column}_ohe" for column in CATEGORICAL_COLUMNS]

    indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=index_column,
            handleInvalid="keep",
            stringOrderType="frequencyDesc",
        )
        for column, index_column in zip(CATEGORICAL_COLUMNS, index_columns, strict=True)
    ]
    encoder = OneHotEncoder(
        inputCols=index_columns,
        outputCols=encoded_columns,
        handleInvalid="keep",
        dropLast=False,
    )
    numeric_assembler = VectorAssembler(
        inputCols=list(NUMERIC_COLUMNS),
        outputCol=modeling["numeric_vector_col"],
        handleInvalid="error",
    )
    numeric_scaler = StandardScaler(
        inputCol=modeling["numeric_vector_col"],
        outputCol=modeling["scaled_numeric_col"],
        withMean=False,
        withStd=True,
    )
    final_assembler = VectorAssembler(
        inputCols=[modeling["scaled_numeric_col"], *encoded_columns],
        outputCol=modeling["features_col"],
        handleInvalid="error",
    )
    return Pipeline(stages=[*indexers, encoder, numeric_assembler, numeric_scaler, final_assembler])


def build_binary_pipeline(
    config: dict[str, Any] | None = None,
    *,
    classifier: Any | None = None,
) -> Pipeline:
    """Return one Pipeline containing both fitted preprocessing and classifier."""
    resolved = config or load_config()
    feature_pipeline = build_feature_pipeline(resolved)
    estimator = classifier
    if estimator is None:
        estimator = LogisticRegression(
            labelCol="label",
            featuresCol=resolved["modeling"]["features_col"],
            maxIter=int(resolved["modeling"]["pipeline_smoke_max_iter"]),
            regParam=0.0,
            elasticNetParam=0.0,
            standardization=False,
        )
    return Pipeline(stages=[*feature_pipeline.getStages(), estimator])


def _label_distribution(dataframe: DataFrame) -> list[dict[str, int]]:
    return [
        {"label": int(row["label"]), "count": int(row["count"])}
        for row in dataframe.groupBy("label").count().orderBy("label").collect()
    ]


def _render_markdown(report: dict[str, Any]) -> str:
    split = report["split"]
    pipeline = report["pipeline_smoke"]
    lines = [
        "# Spark ML feature pipeline — Phase 4",
        "",
        f"Sinh lúc: `{report['generated_at']}`.",
        "",
        "## Chia dữ liệu và chống leakage",
        "",
        f"- Silver train gốc: **{split['source_train_rows']}** dòng.",
        f"- Modeling train: **{split['train_rows']}** dòng.",
        f"- Validation: **{split['validation_rows']}** dòng.",
        f"- Designated test giữ nguyên **{split['designated_test_rows']}** dòng và không được transform/evaluate.",
        f"- Giao nhau `id` train/validation: **{split['train_validation_id_overlap']}**.",
        f"- Cách chia: `{split['method']}`, seed `{split['seed']}`.",
        "",
        "Mọi estimator (`StringIndexer`, `OneHotEncoder`, `StandardScaler`, classifier) chỉ được `fit()` trên modeling train. Validation chỉ đi qua `PipelineModel.transform()`; `attack_cat` và `id` không nằm trong feature vector.",
        "",
        "## Pipeline smoke",
        "",
        f"- Số stages: **{len(pipeline['stage_types'])}**.",
        f"- Stages: {', '.join(f'`{stage}`' for stage in pipeline['stage_types'])}.",
        f"- Feature vector dimension: **{pipeline['feature_dimension']}**.",
        f"- Validation transformed: **{pipeline['validation_prediction_rows']}** dòng.",
        f"- Thời gian fit smoke LR 1 iteration: **{pipeline['fit_seconds']:.3f} giây**.",
        "",
        "Smoke model chỉ chứng minh pipeline kỹ thuật và chưa phải baseline được đánh giá. Hyperparameter, threshold và metrics sẽ được xử lý ở Phase 5.",
        "",
    ]
    return "\n".join(lines)


def run_pipeline_smoke(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path or PROJECT_ROOT / "configs" / "default.yaml")
    spark = create_spark_session(config, app_name="unsw-nb15-phase4-pipeline")
    silver_root = PROJECT_ROOT / config["paths"]["silver"]
    modeling_root = silver_root / "modeling"
    modeling = config["modeling"]

    try:
        source_train = spark.read.parquet(str(silver_root / "train")).cache()
        designated_test_rows = spark.read.parquet(str(silver_root / "test")).count()
        training, validation = split_train_validation(
            source_train,
            validation_fraction=float(modeling["validation_fraction"]),
            seed=int(config["project"]["seed"]),
            hash_buckets=int(modeling["split_hash_buckets"]),
        )
        training = training.cache()
        validation = validation.cache()
        source_train_rows = source_train.count()
        train_rows = training.count()
        validation_rows = validation.count()
        id_overlap = training.select("id").join(validation.select("id"), on="id", how="inner").count()
        if train_rows + validation_rows != source_train_rows or id_overlap != 0:
            raise ValueError("Deterministic train/validation split integrity check failed.")

        training_path = modeling_root / "train"
        validation_path = modeling_root / "validation"
        training.write.mode("overwrite").option("compression", "snappy").parquet(str(training_path))
        validation.write.mode("overwrite").option("compression", "snappy").parquet(str(validation_path))
        reloaded_train = spark.read.parquet(str(training_path))
        reloaded_validation = spark.read.parquet(str(validation_path))
        if reloaded_train.count() != train_rows or reloaded_validation.count() != validation_rows:
            raise ValueError("Persisted modeling split row counts do not match.")

        pipeline = build_binary_pipeline(config)
        fit_started = time.perf_counter()
        pipeline_model = pipeline.fit(reloaded_train)
        fit_seconds = time.perf_counter() - fit_started
        transformed = pipeline_model.transform(reloaded_validation).cache()
        prediction_rows = transformed.count()
        feature_row = transformed.select(modeling["features_col"]).first()
        feature_dimension = int(feature_row[modeling["features_col"]].size)
        required_output_columns = {"features", "rawPrediction", "probability", "prediction", "label"}
        missing_output_columns = sorted(required_output_columns - set(transformed.columns))
        if prediction_rows != validation_rows or missing_output_columns:
            raise ValueError(f"Pipeline output validation failed: {missing_output_columns}")

        report = {
            "phase": 4,
            "status": "ok",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "spark_version": spark.version,
            "feature_contract": {
                "categorical_columns": list(CATEGORICAL_COLUMNS),
                "numeric_columns": list(NUMERIC_COLUMNS),
                "excluded_columns": ["id", "attack_cat", "label"],
                "features_col": modeling["features_col"],
                "string_indexer_handle_invalid": "keep",
                "one_hot_encoder_handle_invalid": "keep",
                "one_hot_encoder_drop_last": False,
                "numeric_scaler_with_mean": False,
                "numeric_scaler_with_std": True,
            },
            "split": {
                "method": "pmod(xxhash64(id, seed), hash_buckets)",
                "seed": int(config["project"]["seed"]),
                "validation_fraction_target": float(modeling["validation_fraction"]),
                "source_train_rows": source_train_rows,
                "train_rows": train_rows,
                "validation_rows": validation_rows,
                "actual_validation_fraction": validation_rows / source_train_rows,
                "designated_test_rows": designated_test_rows,
                "designated_test_untouched": True,
                "train_validation_id_overlap": id_overlap,
                "train_label_distribution": _label_distribution(training),
                "validation_label_distribution": _label_distribution(validation),
                "training_path": str(training_path),
                "validation_path": str(validation_path),
            },
            "pipeline_smoke": {
                "purpose": "Technical fit/transform smoke only; not baseline evaluation.",
                "classifier": "LogisticRegression",
                "classifier_max_iter": int(modeling["pipeline_smoke_max_iter"]),
                "fit_dataset": "modeling/train",
                "transform_dataset": "modeling/validation",
                "fit_seconds": fit_seconds,
                "stage_types": [stage.__class__.__name__ for stage in pipeline_model.stages],
                "feature_dimension": feature_dimension,
                "features_type": transformed.schema[modeling["features_col"]].dataType.simpleString(),
                "validation_prediction_rows": prediction_rows,
                "required_output_columns": sorted(required_output_columns),
                "missing_output_columns": missing_output_columns,
            },
        }
        transformed.unpersist()
        training.unpersist()
        validation.unpersist()
        source_train.unpersist()
    finally:
        spark.stop()

    metrics_path = PROJECT_ROOT / config["paths"]["outputs"] / "metrics" / "phase4_pipeline.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    document_path = PROJECT_ROOT / "docs" / "FEATURE_PIPELINE.md"
    document_path.write_text(_render_markdown(report), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase-4 leakage-safe Spark ML pipeline smoke.")
    parser.add_argument("--config", type=Path, default=None, help="Optional YAML configuration path.")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_pipeline_smoke(arguments.config)

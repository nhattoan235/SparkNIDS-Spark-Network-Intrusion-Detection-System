"""Create the project's consistently configured local Spark session."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Mapping

import pyspark
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "default.yaml"
WINDOWS_LOCAL_FS_JAR = PROJECT_ROOT / ".spark-jars" / "hadoop-bare-naked-local-fs-0.1.0.jar"


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load and minimally validate the central YAML configuration."""
    path = Path(config_path).expanduser().resolve()
    with path.open("r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)

    if not isinstance(config, dict):
        raise ValueError(f"Configuration must be a mapping: {path}")
    for required_section in ("project", "spark", "paths"):
        if not isinstance(config.get(required_section), dict):
            raise ValueError(f"Missing configuration section: {required_section}")
    return config


def configure_runtime_environment() -> None:
    """Select project-local runtimes before launching the Spark JVM."""
    local_jdk_root = PROJECT_ROOT / ".jdk"
    local_jdks = sorted(
        (candidate for candidate in local_jdk_root.glob("jdk-*") if (candidate / "bin" / "java.exe").is_file()),
        reverse=True,
    )
    if local_jdks:
        java_home = str(local_jdks[0].resolve())
        os.environ["JAVA_HOME"] = java_home
        java_bin = str(Path(java_home) / "bin")
        path_entries = os.environ.get("PATH", "").split(os.pathsep)
        if java_bin.casefold() not in {entry.casefold() for entry in path_entries}:
            os.environ["PATH"] = os.pathsep.join([java_bin, *path_entries])

    # Keep driver and Python workers on the same interpreter and avoid common
    # Windows hostname resolution failures in local mode.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def create_spark_session(
    config: Mapping[str, Any] | None = None,
    *,
    app_name: str | None = None,
) -> SparkSession:
    """Build a local SparkSession using only centrally declared settings."""
    resolved = dict(config or load_config())
    spark_config = resolved["spark"]

    configure_runtime_environment()

    builder = (
        SparkSession.builder.appName(app_name or spark_config["app_name"])
        .master(spark_config["master"])
    )
    for key, value in spark_config.get("settings", {}).items():
        builder = builder.config(key, str(value))
    if os.name == "nt" and WINDOWS_LOCAL_FS_JAR.is_file():
        # Hadoop's standard local filesystem requires winutils.exe for POSIX-like
        # permissions. This pure-Java implementation keeps local development free
        # of unsigned native executables while preserving Spark file I/O.
        builder = builder.config("spark.driver.extraClassPath", str(WINDOWS_LOCAL_FS_JAR.resolve()))

    spark = builder.getOrCreate()
    if os.name == "nt" and WINDOWS_LOCAL_FS_JAR.is_file():
        # Set this only after Spark has loaded the driver classpath. Setting it as
        # a launch property would make Spark use the class while resolving its own
        # dependencies, before the class is available.
        hadoop_configuration = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_configuration.set(
            "fs.file.impl",
            "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem",
        )
        spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.closeAll()
    spark.sparkContext.setLogLevel(spark_config.get("log_level", "WARN"))
    return spark


def run_smoke_test(hold_seconds: float = 0.0) -> dict[str, Any]:
    """Run transformations and actions that prove the local Spark stack works."""
    spark = create_spark_session(app_name="unsw-nb15-phase1-smoke")
    try:
        source = spark.createDataFrame(
            [
                (1, "Normal", 100.0),
                (2, "Attack", 250.0),
                (3, "Attack", 150.0),
            ],
            schema="flow_id long, traffic_class string, bytes double",
        ).repartition(2)

        summary_rows = (
            source.filter(F.col("bytes") > 0)
            .groupBy("traffic_class")
            .agg(F.count("*").alias("flow_count"), F.sum("bytes").alias("total_bytes"))
            .orderBy("traffic_class")
            .collect()
        )
        result = {
            "status": "ok",
            "pyspark_version": pyspark.__version__,
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "application_id": spark.sparkContext.applicationId,
            "spark_ui_url": spark.sparkContext.uiWebUrl,
            "input_partitions": source.rdd.getNumPartitions(),
            "input_rows": source.count(),
            "summary": [row.asDict(recursive=True) for row in summary_rows],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        if hold_seconds > 0:
            print(f"Spark UI remains available for {hold_seconds:g} seconds.")
            time.sleep(hold_seconds)
        return result
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the phase-1 Spark smoke test.")
    parser.add_argument(
        "--hold-seconds",
        type=float,
        default=0.0,
        help="Keep Spark alive briefly so the local Spark UI can be inspected.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_smoke_test(hold_seconds=arguments.hold_seconds)

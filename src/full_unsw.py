"""Source audit helpers for the complete four-file UNSW-NB15 dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.unsw_nb15_schema import EXPECTED_COLUMNS


EXPECTED_RAW_FILES = (
    "UNSW-NB15_1.csv",
    "UNSW-NB15_2.csv",
    "UNSW-NB15_3.csv",
    "UNSW-NB15_4.csv",
)

ZENODO_RECORD_URL = "https://zenodo.org/records/10140548"
ZENODO_FILE_METADATA = {
    "UNSW-NB15_1.csv": {"size": 168_979_718, "md5": "560dcc5981b3f6ab98027a16b053c9c1"},
    "UNSW-NB15_2.csv": {"size": 165_221_021, "md5": "59215357fbed7c76efc6a404a8607688"},
    "UNSW-NB15_3.csv": {"size": 154_588_103, "md5": "75e8567341edb760a23f803b920b69d4"},
    "UNSW-NB15_4.csv": {"size": 97_588_754, "md5": "f55875cab7f037fdad40b5b737e7db5e"},
}

RAW_STRING_COLUMNS = {
    "srcip",
    "sport",
    "dstip",
    "dsport",
    "proto",
    "state",
    "service",
    "attack_cat",
}
RAW_PORT_COLUMNS = {"sport", "dsport"}
RAW_LONG_COLUMNS = {"stcpb", "dtcpb", "res_bdy_len", "Stime", "Ltime"}
RAW_DOUBLE_COLUMNS = {
    "dur",
    "Sload",
    "Dload",
    "Sjit",
    "Djit",
    "Sintpkt",
    "Dintpkt",
    "tcprtt",
    "synack",
    "ackdat",
}
RAW_COLUMNS = (
    "srcip",
    "sport",
    "dstip",
    "dsport",
    "proto",
    "state",
    "dur",
    "sbytes",
    "dbytes",
    "sttl",
    "dttl",
    "sloss",
    "dloss",
    "service",
    "Sload",
    "Dload",
    "Spkts",
    "Dpkts",
    "swin",
    "dwin",
    "stcpb",
    "dtcpb",
    "smeansz",
    "dmeansz",
    "trans_depth",
    "res_bdy_len",
    "Sjit",
    "Djit",
    "Stime",
    "Ltime",
    "Sintpkt",
    "Dintpkt",
    "tcprtt",
    "synack",
    "ackdat",
    "is_sm_ips_ports",
    "ct_state_ttl",
    "ct_flw_http_mthd",
    "is_ftp_login",
    "ct_ftp_cmd",
    "ct_srv_src",
    "ct_srv_dst",
    "ct_dst_ltm",
    "ct_src_ltm",
    "ct_src_dport_ltm",
    "ct_dst_sport_ltm",
    "ct_dst_src_ltm",
    "attack_cat",
    "label",
)
RAW_NUMERIC_COLUMNS = tuple(name for name in RAW_COLUMNS if name not in RAW_STRING_COLUMNS or name in RAW_PORT_COLUMNS)


def build_raw_schema() -> StructType:
    """Keep every source token as text so hex ports and blank cells parse losslessly."""
    return StructType([StructField(name, StringType(), nullable=True) for name in RAW_COLUMNS])


def _raw_numeric_value(name: str):
    """Safely parse one numeric token; malformed values become auditable nulls, not job failures."""
    if name in RAW_PORT_COLUMNS:
        token = F.lower(F.trim(F.col(name)))
        decimal_value = F.expr(f"try_cast(trim(`{name}`) AS BIGINT)")
        return F.when(
            token.rlike(r"^0x[0-9a-f]+$"),
            F.conv(F.substring(token, 3, 64), 16, 10).cast("long"),
        ).otherwise(decimal_value)
    target_type = "double" if name in RAW_DOUBLE_COLUMNS else "bigint" if name in RAW_LONG_COLUMNS else "int"
    return F.expr(f"try_cast(trim(`{name}`) AS {target_type.upper()})")


def read_raw_csv(spark: SparkSession, path: str | Path) -> DataFrame:
    """Read one headerless source CSV without coercing source text prematurely."""
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"UNSW-NB15 source CSV does not exist: {source}")
    return (
        spark.read.schema(build_raw_schema())
        .option("header", "false")
        .option("mode", "FAILFAST")
        .option("enforceSchema", "true")
        .csv(str(source))
        .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/\\]+)$", 1))
    )


def read_full_raw(spark: SparkSession, root: str | Path) -> DataFrame:
    """Read and union the four raw files before assigning canonical row IDs."""
    directory = Path(root).expanduser().resolve()
    missing = [name for name in EXPECTED_RAW_FILES if not (directory / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing UNSW-NB15 source file(s): {', '.join(missing)}")
    frames = [read_raw_csv(spark, directory / name) for name in EXPECTED_RAW_FILES]
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def canonicalize_raw(dataframe: DataFrame, *, source_file: str | None = None) -> DataFrame:
    """Map raw rows to the 45-column model contract and retain row-level quality flags."""
    missing = sorted(set(RAW_COLUMNS) - set(dataframe.columns))
    if missing:
        raise ValueError(f"Raw UNSW-NB15 schema is missing columns: {missing}")
    if source_file is None and "source_file" not in dataframe.columns:
        raise ValueError("source_file is required when canonicalizing a raw UNSW-NB15 DataFrame")

    fingerprint_columns = []
    for name in RAW_COLUMNS:
        if name in {"attack_cat", "label"}:
            continue
        if name in RAW_NUMERIC_COLUMNS:
            value = _raw_numeric_value(name)
        else:
            value = F.lower(F.trim(F.col(name)))
            value = F.when(value == "", F.lit(None)).otherwise(value)
        fingerprint_columns.append(value.alias(name))
    fingerprint = F.sha2(F.to_json(F.struct(*fingerprint_columns), options={"ignoreNullFields": "false"}), 256)
    parse_errors = []
    missing_numeric = []
    for name in RAW_NUMERIC_COLUMNS:
        token = F.trim(F.col(name))
        parsed = _raw_numeric_value(name)
        is_missing = F.col(name).isNull() | (token == "")
        parse_errors.append(F.when(~is_missing & parsed.isNull(), F.lit(name)))
        missing_numeric.append(F.when(is_missing, F.lit(name)))
    parse_error_fields = F.array_compact(F.array(*parse_errors))
    missing_numeric_fields = F.array_compact(F.array(*missing_numeric))
    duration = _raw_numeric_value("dur")
    source_packets = _raw_numeric_value("Spkts")
    destination_packets = _raw_numeric_value("Dpkts")
    rate = F.when(
        duration > 0,
        (source_packets.cast("double") + destination_packets.cast("double")) / duration,
    ).otherwise(F.lit(0.0))
    file_column = F.lit(source_file) if source_file else F.col("source_file")
    canonical = dataframe.select(
        (F.monotonically_increasing_id() + F.lit(1)).cast("long").alias("id"),
        _raw_numeric_value("dur").cast("double").alias("dur"),
        F.col("proto").alias("proto"),
        F.col("service").alias("service"),
        F.col("state").alias("state"),
        _raw_numeric_value("Spkts").alias("spkts"),
        _raw_numeric_value("Dpkts").alias("dpkts"),
        _raw_numeric_value("sbytes").cast("long").alias("sbytes"),
        _raw_numeric_value("dbytes").cast("long").alias("dbytes"),
        rate.cast("double").alias("rate"),
        _raw_numeric_value("sttl").alias("sttl"),
        _raw_numeric_value("dttl").alias("dttl"),
        _raw_numeric_value("Sload").alias("sload"),
        _raw_numeric_value("Dload").alias("dload"),
        _raw_numeric_value("sloss").alias("sloss"),
        _raw_numeric_value("dloss").alias("dloss"),
        _raw_numeric_value("Sintpkt").alias("sinpkt"),
        _raw_numeric_value("Dintpkt").alias("dinpkt"),
        _raw_numeric_value("Sjit").alias("sjit"),
        _raw_numeric_value("Djit").alias("djit"),
        _raw_numeric_value("swin").alias("swin"),
        _raw_numeric_value("stcpb").alias("stcpb"),
        _raw_numeric_value("dtcpb").alias("dtcpb"),
        _raw_numeric_value("dwin").alias("dwin"),
        _raw_numeric_value("tcprtt").alias("tcprtt"),
        _raw_numeric_value("synack").alias("synack"),
        _raw_numeric_value("ackdat").alias("ackdat"),
        _raw_numeric_value("smeansz").alias("smean"),
        _raw_numeric_value("dmeansz").alias("dmean"),
        _raw_numeric_value("trans_depth").alias("trans_depth"),
        _raw_numeric_value("res_bdy_len").cast("long").alias("response_body_len"),
        _raw_numeric_value("ct_srv_src").alias("ct_srv_src"),
        _raw_numeric_value("ct_state_ttl").alias("ct_state_ttl"),
        _raw_numeric_value("ct_dst_ltm").alias("ct_dst_ltm"),
        _raw_numeric_value("ct_src_dport_ltm").alias("ct_src_dport_ltm"),
        _raw_numeric_value("ct_dst_sport_ltm").alias("ct_dst_sport_ltm"),
        _raw_numeric_value("ct_dst_src_ltm").alias("ct_dst_src_ltm"),
        _raw_numeric_value("is_ftp_login").alias("is_ftp_login"),
        _raw_numeric_value("ct_ftp_cmd").alias("ct_ftp_cmd"),
        _raw_numeric_value("ct_flw_http_mthd").alias("ct_flw_http_mthd"),
        _raw_numeric_value("ct_src_ltm").alias("ct_src_ltm"),
        _raw_numeric_value("ct_srv_dst").alias("ct_srv_dst"),
        _raw_numeric_value("is_sm_ips_ports").alias("is_sm_ips_ports"),
        F.when(_raw_numeric_value("label") == 0, F.lit("Normal"))
        .otherwise(F.trim(F.col("attack_cat")))
        .alias("attack_cat"),
        _raw_numeric_value("label").alias("label"),
        fingerprint.alias("flow_fingerprint"),
        file_column.cast("string").alias("source_file"),
        parse_error_fields.alias("raw_parse_error_fields"),
        missing_numeric_fields.alias("raw_missing_numeric_fields"),
    )
    missing_canonical = sorted(set(EXPECTED_COLUMNS) - set(canonical.columns))
    if missing_canonical:
        raise ValueError(f"Canonical UNSW-NB15 mapping is incomplete: {missing_canonical}")
    return canonical


def audit_canonical_frame(dataframe: DataFrame) -> dict[str, Any]:
    """Count records, duplicate feature fingerprints, label conflicts, and source distribution."""
    required = {"flow_fingerprint", "label"}
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise ValueError(f"Cannot audit full UNSW-NB15 rows without columns: {missing}")

    row_count = dataframe.count()
    groups = dataframe.groupBy("flow_fingerprint").agg(
        F.count(F.lit(1)).alias("_rows"),
        F.countDistinct("label").alias("_labels"),
        F.max(F.when(F.col("label").isNull(), 1).otherwise(0)).alias("_has_null_label"),
    ).withColumn("_label_versions", F.col("_labels") + F.col("_has_null_label"))
    group_counts = groups.agg(
        F.count(F.lit(1)).alias("unique_flows"),
        F.sum(F.col("_rows") - 1).alias("duplicate_rows"),
        F.sum(F.when(F.col("_label_versions") > 1, 1).otherwise(0)).alias(
            "conflicting_fingerprint_groups"
        ),
        F.sum(F.when(F.col("_label_versions") > 1, F.col("_rows")).otherwise(0)).alias(
            "conflicting_rows"
        ),
    ).first().asDict()
    label_counts = {
        str(row["label"] if row["label"] is not None else "null"): int(row["count"])
        for row in dataframe.groupBy("label").count().collect()
    }
    invalid_label_rows = dataframe.filter(
        F.col("label").isNull() | ~F.col("label").isin(0, 1)
    ).count()
    source_counts = {}
    if "source_file" in dataframe.columns:
        source_counts = {
            str(row["source_file"]): int(row["count"])
            for row in dataframe.groupBy("source_file").count().orderBy("source_file").collect()
        }
    malformed_numeric_counts: dict[str, int] = {}
    missing_numeric_counts: dict[str, int] = {}
    quality_columns = []
    if "raw_parse_error_fields" in dataframe.columns:
        quality_columns.extend(
            F.sum(
                F.when(F.array_contains("raw_parse_error_fields", name), 1).otherwise(0)
            ).alias(f"malformed__{name}")
            for name in RAW_NUMERIC_COLUMNS
        )
    if "raw_missing_numeric_fields" in dataframe.columns:
        quality_columns.extend(
            F.sum(
                F.when(F.array_contains("raw_missing_numeric_fields", name), 1).otherwise(0)
            ).alias(f"missing__{name}")
            for name in RAW_NUMERIC_COLUMNS
        )
    if quality_columns:
        quality = dataframe.agg(*quality_columns).first().asDict()
        malformed_numeric_counts = {
            key.removeprefix("malformed__"): int(value)
            for key, value in quality.items()
            if key.startswith("malformed__") and value
        }
        missing_numeric_counts = {
            key.removeprefix("missing__"): int(value)
            for key, value in quality.items()
            if key.startswith("missing__") and value
        }
    return {
        "rows": row_count,
        "unique_flows": int(group_counts["unique_flows"] or 0),
        "duplicate_rows": int(group_counts["duplicate_rows"] or 0),
        "conflicting_fingerprint_groups": int(group_counts["conflicting_fingerprint_groups"] or 0),
        "conflicting_rows": int(group_counts["conflicting_rows"] or 0),
        "invalid_label_rows": invalid_label_rows,
        "malformed_numeric_counts": malformed_numeric_counts,
        "missing_numeric_counts": missing_numeric_counts,
        "label_counts": label_counts,
        "source_counts": source_counts,
    }


def split_full_data(dataframe: DataFrame, *, seed: int = 42) -> dict[str, DataFrame]:
    """Create deterministic 70/15/15 splits while keeping duplicate flows together."""
    required = {"flow_fingerprint", "label"}
    missing = sorted(required - set(dataframe.columns))
    if missing:
        raise ValueError(f"Cannot split full UNSW-NB15 data without columns: {missing}")

    single_label_flows = (
        dataframe.groupBy("flow_fingerprint")
        .agg(F.countDistinct("label").alias("_label_count"))
        .filter(F.col("_label_count") == 1)
        .select("flow_fingerprint")
    )
    eligible_rows = (
        dataframe.join(single_label_flows, on="flow_fingerprint", how="left_semi")
        .withColumn("_split_bucket", F.pmod(F.xxhash64("flow_fingerprint", F.lit(int(seed))), F.lit(100)))
    )
    return {
        "train": eligible_rows.filter(F.col("_split_bucket") < 70).drop("_split_bucket"),
        "validation": eligible_rows.filter(F.col("_split_bucket").between(70, 84)).drop("_split_bucket"),
        "test": eligible_rows.filter(F.col("_split_bucket") >= 85).drop("_split_bucket"),
    }


def _file_hashes(path: Path) -> tuple[str, str]:
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            md5.update(block)
            sha256.update(block)
    return md5.hexdigest(), sha256.hexdigest()


def audit_sources(
    root: str | Path,
    *,
    expected_metadata: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Verify that all four source CSVs exist and return bounded file provenance."""
    directory = Path(root).expanduser().resolve()
    if not directory.is_dir():
        raise FileNotFoundError(f"UNSW-NB15 source directory does not exist: {directory}")

    missing = [name for name in EXPECTED_RAW_FILES if not (directory / name).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing UNSW-NB15 source file(s): {', '.join(missing)}")

    files = []
    for name in EXPECTED_RAW_FILES:
        path = directory / name
        size = path.stat().st_size
        if size <= 0:
            raise ValueError(f"UNSW-NB15 source file is empty: {name}")
        md5, sha256 = _file_hashes(path)
        expected = (expected_metadata or {}).get(name)
        if expected:
            if size != int(expected["size"]) or md5.casefold() != str(expected["md5"]).casefold():
                raise ValueError(
                    f"Source checksum/size mismatch for {name}: "
                    f"got {size} bytes, MD5 {md5}; expected {expected['size']} bytes, MD5 {expected['md5']}"
                )
        files.append({"name": name, "bytes": size, "md5": md5, "sha256": sha256})

    return {
        "status": "verified",
        "source": "Zenodo mirror of UNSW-NB15 raw CSVs" if expected_metadata else "UNSW-NB15 raw CSVs",
        "record_url": ZENODO_RECORD_URL if expected_metadata == ZENODO_FILE_METADATA else None,
        "root": str(directory),
        "file_count": len(files),
        "total_bytes": sum(item["bytes"] for item in files),
        "files": files,
        "audited_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit the complete four-file UNSW-NB15 CSV source.")
    parser.add_argument("--root", required=True, type=Path, help="Folder containing the four raw CSV files.")
    parser.add_argument("--manifest", type=Path, help="Optional path to write the verified source manifest.")
    arguments = parser.parse_args()

    report = audit_sources(arguments.root, expected_metadata=ZENODO_FILE_METADATA)
    if arguments.manifest:
        destination = arguments.manifest.expanduser().resolve()
        if destination.exists():
            raise FileExistsError(f"Refusing to overwrite existing source manifest: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".tmp")
        temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        temporary.replace(destination)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

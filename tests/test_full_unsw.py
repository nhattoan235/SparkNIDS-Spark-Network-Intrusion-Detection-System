from __future__ import annotations

import hashlib
import json

import pytest

from src.full_unsw import (
    EXPECTED_RAW_FILES,
    RAW_DOUBLE_COLUMNS,
    RAW_COLUMNS,
    RAW_STRING_COLUMNS,
    audit_sources,
    audit_canonical_frame,
    build_raw_schema,
    canonicalize_raw,
    read_raw_csv,
    read_full_raw,
    split_full_data,
)
from src.spark_session import create_spark_session
from src.full_unsw_pipeline import _json_for_console, confusion_counts, source_count_audit


def test_full_pipeline_console_json_handles_legacy_windows_encoding():
    payload = {"example": "bắt đúng tấn công"}

    rendered = _json_for_console(payload, encoding="cp1252")

    assert rendered.encode("cp1252")
    assert json.loads(rendered) == payload


def test_full_pipeline_console_json_keeps_vietnamese_in_utf8_terminal():
    payload = {"example": "bắt đúng tấn công"}

    rendered = _json_for_console(payload, encoding="utf-8")

    assert "bắt đúng tấn công" in rendered


def _write_dataset_files(root, *, content: bytes = b"flow,label\n1,0\n") -> None:
    root.mkdir(parents=True, exist_ok=True)
    for filename in EXPECTED_RAW_FILES:
        (root / filename).write_bytes(content)


def test_audit_sources_reports_size_and_sha256_for_all_four_files(tmp_path):
    _write_dataset_files(tmp_path)

    report = audit_sources(tmp_path)

    assert report["file_count"] == 4
    assert report["total_bytes"] == 4 * len(b"flow,label\n1,0\n")
    assert {item["name"] for item in report["files"]} == set(EXPECTED_RAW_FILES)
    assert all(item["sha256"] == hashlib.sha256(b"flow,label\n1,0\n").hexdigest() for item in report["files"])


def test_audit_sources_names_every_missing_file(tmp_path):
    (tmp_path / EXPECTED_RAW_FILES[0]).write_bytes(b"present")

    with pytest.raises(FileNotFoundError) as error:
        audit_sources(tmp_path)

    assert all(name in str(error.value) for name in EXPECTED_RAW_FILES[1:])


def test_audit_sources_rejects_empty_csv(tmp_path):
    _write_dataset_files(tmp_path)
    (tmp_path / EXPECTED_RAW_FILES[2]).write_bytes(b"")

    with pytest.raises(ValueError, match=EXPECTED_RAW_FILES[2]):
        audit_sources(tmp_path)


def test_audit_sources_checks_published_mirror_metadata(tmp_path):
    _write_dataset_files(tmp_path)
    expected = {EXPECTED_RAW_FILES[0]: {"size": 1, "md5": "wrong"}}

    with pytest.raises(ValueError, match=EXPECTED_RAW_FILES[0]):
        audit_sources(tmp_path, expected_metadata=expected)


def test_raw_schema_and_mapping_match_the_headerless_49_column_files():
    schema = build_raw_schema()

    assert len(schema.fields) == 49
    assert schema.fieldNames()[:6] == ["srcip", "sport", "dstip", "dsport", "proto", "state"]
    assert schema.fieldNames()[-2:] == ["attack_cat", "label"]
    assert all(field.dataType.simpleString() == "string" for field in schema.fields)


def test_read_raw_csv_parses_headerless_rows_and_records_source_file(tmp_path):
    schema = build_raw_schema()
    values = {}
    for name in RAW_COLUMNS:
        values[name] = (
            "Generic" if name == "attack_cat" else
            "192.0.2.1" if name == "srcip" else
            "198.51.100.4" if name == "dstip" else
            "tcp" if name in RAW_STRING_COLUMNS else
            "1.5" if name in RAW_DOUBLE_COLUMNS else
            "1"
        )
    values.update({"sport": "0xc0a8", "dsport": "0x0050", "proto": "tcp", "state": "FIN", "service": "http", "ct_ftp_cmd": " "})
    source = tmp_path / "UNSW-NB15_1.csv"
    source.write_text(",".join(str(values[name]) for name in RAW_COLUMNS) + "\n", encoding="utf-8")

    spark = create_spark_session(app_name="full-unsw-raw-csv-test")
    try:
        parsed = read_raw_csv(spark, source)

        assert parsed.count() == 1
        assert parsed.columns == [*RAW_COLUMNS, "source_file"]
        row = parsed.first()
        assert row.source_file == source.name
        assert row.sport == "0xc0a8"
        assert row.ct_ftp_cmd == " "
    finally:
        spark.stop()


def test_read_full_raw_unions_files_and_assigns_unique_ids(tmp_path):
    schema = build_raw_schema()
    values = {}
    for field in schema.fields:
        kind = field.dataType.simpleString()
        values[field.name] = "Generic" if field.name == "attack_cat" else "tcp" if kind == "string" else 1.5 if kind == "double" else 1
    values.update({"srcip": "192.0.2.1", "dstip": "198.51.100.4", "proto": "tcp", "state": "FIN", "service": "http", "label": 1})
    row = ",".join(str(values[name]) for name in RAW_COLUMNS) + "\n"
    for filename in EXPECTED_RAW_FILES:
        (tmp_path / filename).write_text(row, encoding="utf-8")

    spark = create_spark_session(app_name="full-unsw-union-test")
    try:
        raw = read_full_raw(spark, tmp_path)
        canonical = canonicalize_raw(raw)
        rows = canonical.select("id", "source_file").collect()

        assert len(rows) == 4
        assert len({row.id for row in rows}) == 4
        assert {row.source_file for row in rows} == set(EXPECTED_RAW_FILES)
    finally:
        spark.stop()


def test_audit_canonical_frame_counts_duplicates_and_label_conflicts():
    spark = create_spark_session(app_name="full-unsw-quality-audit-test")
    try:
        frame = spark.createDataFrame(
            [
                ("duplicate", 0, "a.csv"),
                ("duplicate", 0, "b.csv"),
                ("conflict", 0, "a.csv"),
                ("conflict", 1, "b.csv"),
                ("unique", 1, "c.csv"),
            ],
            "flow_fingerprint string, label int, source_file string",
        )

        result = audit_canonical_frame(frame)

        assert result["rows"] == 5
        assert result["unique_flows"] == 3
        assert result["duplicate_rows"] == 2
        assert result["conflicting_fingerprint_groups"] == 1
        assert result["conflicting_rows"] == 2
        assert result["label_counts"] == {"0": 3, "1": 2}
    finally:
        spark.stop()


def test_audit_canonical_frame_reports_raw_parse_and_missing_counts():
    spark = create_spark_session(app_name="full-unsw-raw-quality-test")
    try:
        frame = spark.createDataFrame(
            [
                ("a", 0, ["sport"], ["sttl"]),
                ("b", 1, [], []),
            ],
            "flow_fingerprint string, label int, raw_parse_error_fields array<string>, raw_missing_numeric_fields array<string>",
        )

        report = audit_canonical_frame(frame)

        assert report["malformed_numeric_counts"] == {"sport": 1}
        assert report["missing_numeric_counts"] == {"sttl": 1}
    finally:
        spark.stop()


def test_canonicalization_builds_the_existing_45_column_contract_and_rate():
    schema = build_raw_schema()
    values = {field.name: None for field in schema.fields}
    values.update(
        {
            "srcip": "192.0.2.1",
            "sport": "49320",
            "dstip": "198.51.100.4",
            "dsport": "80",
            "proto": "tcp",
            "state": "FIN",
            "dur": 2.0,
            "sbytes": 10,
            "dbytes": 20,
            "sttl": 64,
            "dttl": 64,
            "sloss": 0,
            "dloss": 0,
            "service": "http",
            "Sload": 100.0,
            "Dload": 120.0,
            "Spkts": 2,
            "Dpkts": 3,
            "swin": 0,
            "dwin": 0,
            "stcpb": 0,
            "dtcpb": 0,
            "smeansz": 5,
            "dmeansz": 7,
            "trans_depth": 0,
            "res_bdy_len": 0,
            "Sjit": 0.0,
            "Djit": 0.0,
            "Stime": 1421927414,
            "Ltime": 1421927416,
            "Sintpkt": 1.0,
            "Dintpkt": 1.0,
            "tcprtt": 0.0,
            "synack": 0.0,
            "ackdat": 0.0,
            "is_sm_ips_ports": 0,
            "ct_state_ttl": 1,
            "ct_flw_http_mthd": 1,
            "is_ftp_login": 0,
            "ct_ftp_cmd": 0,
            "ct_srv_src": 1,
            "ct_srv_dst": 1,
            "ct_dst_ltm": 1,
            "ct_src_ltm": 1,
            "ct_src_dport_ltm": 1,
            "ct_dst_sport_ltm": 1,
            "ct_dst_src_ltm": 1,
            "attack_cat": "Generic",
            "label": 1,
        }
    )

    spark = create_spark_session(app_name="full-unsw-canonical-contract-test")
    try:
        values["ct_ftp_cmd"] = " "
        values["ct_flw_http_mthd"] = "not-a-number"
        hex_values = {**values, "sport": "0xc0a8", "dsport": "0x0050"}
        frame = spark.createDataFrame(
            [tuple(None if record[name] is None else str(record[name]) for name in schema.fieldNames()) for record in (values, hex_values)],
            schema=schema,
        )
        canonical = canonicalize_raw(frame, source_file="UNSW-NB15_1.csv")
        rows = canonical.select(
            "id", "rate", "attack_cat", "label", "flow_fingerprint", "source_file",
            "ct_ftp_cmd", "raw_parse_error_fields", "raw_missing_numeric_fields",
        ).collect()
        row = rows[0]

        assert row.id > 0
        assert row.rate == pytest.approx(2.5)
        assert row.attack_cat == "Generic"
        assert row.label == 1
        assert len(row.flow_fingerprint) == 64
        assert row.source_file == "UNSW-NB15_1.csv"
        assert rows[0].flow_fingerprint == rows[1].flow_fingerprint
        assert row.ct_ftp_cmd is None
        assert "ct_flw_http_mthd" in row.raw_parse_error_fields
        assert "ct_ftp_cmd" in row.raw_missing_numeric_fields
        assert len([column for column in canonical.columns if column in {"srcip", "sport", "dstip", "dsport", "Stime", "Ltime"}]) == 0
        assert len(canonical.columns) == 49
    finally:
        spark.stop()


def test_split_groups_duplicates_and_excludes_conflicting_labels():
    from pyspark.sql import functions as F

    spark = create_spark_session(app_name="full-unsw-split-test")
    try:
        records = [(f"flow-{index:04}", index % 2) for index in range(1000)]
        records.extend([("flow-0000", 0), ("conflict", 0), ("conflict", 1)])
        frame = spark.createDataFrame(records, "flow_fingerprint string, label int")

        first = split_full_data(frame, seed=42)
        shuffled = split_full_data(frame.repartition(5).orderBy(F.desc("flow_fingerprint")), seed=42)

        def assignment(splits):
            result = {}
            for split_name, split in splits.items():
                for row in split.select("flow_fingerprint", "label").distinct().collect():
                    assert row.flow_fingerprint not in result
                    result[row.flow_fingerprint] = (row.label, split_name)
            return result

        expected = assignment(first)
        assert len(expected) == 1000
        assert sum(split.count() for split in first.values()) == 1001
        assert "conflict" not in expected
        assert sum(value[0] == 0 for value in expected.values()) > 0
        assert sum(value[0] == 1 for value in expected.values()) > 0
        assert expected == assignment(shuffled)
    finally:
        spark.stop()


def test_confusion_counts_match_each_test_flow_once():
    spark = create_spark_session(app_name="full-unsw-confusion-counts-test")
    try:
        predictions = spark.createDataFrame(
            [(1, 1), (0, 1), (0, 0), (1, 0)],
            "label int, prediction int",
        )

        counts = confusion_counts(predictions)

        assert counts == {
            "true_positive": 1,
            "false_positive": 1,
            "true_negative": 1,
            "false_negative": 1,
        }
    finally:
        spark.stop()


def test_verified_mirror_count_difference_is_reported_not_silently_dropped():
    result = source_count_audit(actual_rows=2540047, mirror_rows=2540047, published_rows=2540044)

    assert result == {
        "actual_rows": 2540047,
        "verified_mirror_expected_rows": 2540047,
        "published_rows": 2540044,
        "difference_from_published": 3,
    }

    with pytest.raises(ValueError, match="verified mirror count"):
        source_count_audit(actual_rows=2540046, mirror_rows=2540047, published_rows=2540044)

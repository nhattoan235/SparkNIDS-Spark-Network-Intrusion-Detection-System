"""Tests for small, JSON-ready Spark teaching experiments."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from src.demo_experiments import run_format_comparison, summarize_data
from src.spark_session import create_spark_session
from src.unsw_nb15_schema import EXPECTED_COLUMNS


def _write_unsw_csv(path: Path) -> None:
    row = {column: "0" for column in EXPECTED_COLUMNS}
    row.update({"proto": "tcp", "service": "http", "state": "FIN", "attack_cat": "Normal", "label": "0"})
    attack = {**row, "id": "2", "proto": "udp", "attack_cat": "Exploits", "label": "1", "sbytes": "20"}
    normal = {**row, "id": "1", "sbytes": "10"}
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()
        writer.writerows([normal, attack])


def test_summarize_data_returns_bounded_schema_and_label_aggregates() -> None:
    spark = create_spark_session(app_name="guided-demo-summary-test")
    try:
        frame = spark.createDataFrame([(1, 0), (2, 1)], "id int, label int")

        result = summarize_data(frame)

        assert result["row_count"] == 2
        assert result["column_count"] == 2
        assert result["label_distribution"] == [{"label": 0, "count": 1}, {"label": 1, "count": 1}]
        json.dumps(result)
    finally:
        spark.stop()


def test_format_comparison_uses_same_query_and_matches_results(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    parquet_path = tmp_path / "sample.parquet"
    _write_unsw_csv(csv_path)
    spark = create_spark_session(app_name="guided-demo-format-test")
    try:
        spark.read.option("header", True).schema("id int, dur double, proto string, service string, state string, spkts int, dpkts int, sbytes long, dbytes long, rate double, sttl int, dttl int, sload double, dload double, sloss int, dloss int, sinpkt double, dinpkt double, sjit double, djit double, swin int, stcpb long, dtcpb long, dwin int, tcprtt double, synack double, ackdat double, smean int, dmean int, trans_depth int, response_body_len long, ct_srv_src int, ct_state_ttl int, ct_dst_ltm int, ct_src_dport_ltm int, ct_dst_sport_ltm int, ct_dst_src_ltm int, is_ftp_login int, ct_ftp_cmd int, ct_flw_http_mthd int, ct_src_ltm int, ct_srv_dst int, is_sm_ips_ports int, attack_cat string, label int").csv(str(csv_path)).write.mode("overwrite").parquet(str(parquet_path))

        result = run_format_comparison(spark, csv_path, parquet_path, warmups=0, runs=1)

        assert result["identical_results"] is True
        assert result["benchmark"]["csv"]["result_sha256"] == result["benchmark"]["parquet"]["result_sha256"]
        assert result["benchmark"]["csv"]["timing"]["median_seconds"] > 0
        assert result["benchmark"]["parquet"]["timing"]["median_seconds"] > 0
        assert len(result["metrics"]) <= 3
        json.dumps(result)
    finally:
        spark.stop()

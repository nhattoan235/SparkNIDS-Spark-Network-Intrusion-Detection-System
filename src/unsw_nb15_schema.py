"""Explicit schema and column groups for the designated UNSW-NB15 splits."""

from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType


UNSW_NB15_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("dur", DoubleType(), True),
        StructField("proto", StringType(), True),
        StructField("service", StringType(), True),
        StructField("state", StringType(), True),
        StructField("spkts", IntegerType(), True),
        StructField("dpkts", IntegerType(), True),
        StructField("sbytes", LongType(), True),
        StructField("dbytes", LongType(), True),
        StructField("rate", DoubleType(), True),
        StructField("sttl", IntegerType(), True),
        StructField("dttl", IntegerType(), True),
        StructField("sload", DoubleType(), True),
        StructField("dload", DoubleType(), True),
        StructField("sloss", IntegerType(), True),
        StructField("dloss", IntegerType(), True),
        StructField("sinpkt", DoubleType(), True),
        StructField("dinpkt", DoubleType(), True),
        StructField("sjit", DoubleType(), True),
        StructField("djit", DoubleType(), True),
        StructField("swin", IntegerType(), True),
        StructField("stcpb", LongType(), True),
        StructField("dtcpb", LongType(), True),
        StructField("dwin", IntegerType(), True),
        StructField("tcprtt", DoubleType(), True),
        StructField("synack", DoubleType(), True),
        StructField("ackdat", DoubleType(), True),
        StructField("smean", IntegerType(), True),
        StructField("dmean", IntegerType(), True),
        StructField("trans_depth", IntegerType(), True),
        StructField("response_body_len", LongType(), True),
        StructField("ct_srv_src", IntegerType(), True),
        StructField("ct_state_ttl", IntegerType(), True),
        StructField("ct_dst_ltm", IntegerType(), True),
        StructField("ct_src_dport_ltm", IntegerType(), True),
        StructField("ct_dst_sport_ltm", IntegerType(), True),
        StructField("ct_dst_src_ltm", IntegerType(), True),
        StructField("is_ftp_login", IntegerType(), True),
        StructField("ct_ftp_cmd", IntegerType(), True),
        StructField("ct_flw_http_mthd", IntegerType(), True),
        StructField("ct_src_ltm", IntegerType(), True),
        StructField("ct_srv_dst", IntegerType(), True),
        StructField("is_sm_ips_ports", IntegerType(), True),
        StructField("attack_cat", StringType(), True),
        StructField("label", IntegerType(), False),
    ]
)

EXPECTED_COLUMNS = tuple(field.name for field in UNSW_NB15_SCHEMA.fields)
CATEGORICAL_COLUMNS = ("proto", "service", "state")
TARGET_COLUMNS = ("attack_cat", "label")
IDENTIFIER_COLUMNS = ("id",)
NUMERIC_COLUMNS = tuple(
    field.name
    for field in UNSW_NB15_SCHEMA.fields
    if field.name not in {*CATEGORICAL_COLUMNS, *TARGET_COLUMNS, *IDENTIFIER_COLUMNS}
)


import pytest

from src.lab_features import SESSION_FEATURES, aggregate_sessions
from src.spark_session import create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = create_spark_session(app_name="lab-session-features-test")
    yield session
    session.stop()


def test_spark_builds_one_time_aware_row_per_run_without_labels(spark):
    flows = spark.createDataFrame(
        [
            ("a", "run-1", "demo", "flow", "2026-09-01T00:00:00Z", 8080, "http", 10, 5, 0.01),
            ("b", "run-1", "demo", "flow", "2026-09-01T00:00:00.5Z", 8080, "http", 10, 5, 0.01),
            ("c", "run-1", "demo", "flow", "2026-09-01T00:00:01Z", 9001, "-", 8, 3, 0.02),
        ],
        "event_id string, run_id string, lab_mode string, record_type string, timestamp string, "
        "destination_port int, service string, sbytes long, dbytes long, dur double",
    )

    rows = aggregate_sessions(flows).collect()

    assert len(rows) == 1
    assert rows[0].run_id == "run-1"
    assert rows[0].flow_count == 3
    assert rows[0].destination_port_count == 2
    assert rows[0].elapsed_seconds == pytest.approx(1.0)
    assert rows[0].flow_rate == pytest.approx(3.0)
    assert set(SESSION_FEATURES) <= rows[0].asDict().keys()
    assert "label" not in rows[0].asDict()

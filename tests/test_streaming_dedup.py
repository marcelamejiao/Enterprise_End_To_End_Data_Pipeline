"""Unit tests for the Pipeline B dedup/validation logic, run against a local
Spark session (no Kafka cluster needed) so this runs in plain CI.
"""

from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import Row, SparkSession  # noqa: E402

from streaming.spark_consumer import dedup_and_validate  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("test-dedup")
        .master("local[2]")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_drops_duplicate_event_ids(spark):
    rows = [
        Row(event_id="a", order_id="1", event_type="purchase", event_ts=datetime(2026, 1, 1, 12, 0)),
        Row(event_id="a", order_id="1", event_type="purchase", event_ts=datetime(2026, 1, 1, 12, 0)),
        Row(event_id="b", order_id="2", event_type="purchase", event_ts=datetime(2026, 1, 1, 12, 1)),
    ]
    df = spark.createDataFrame(rows)
    result = dedup_and_validate(df)
    assert set(r.event_id for r in result.collect()) == {"a", "b"}


def test_drops_rows_with_null_required_fields(spark):
    rows = [
        Row(event_id=None, order_id="1", event_type="purchase", event_ts=datetime(2026, 1, 1, 12, 0)),
        Row(event_id="c", order_id=None, event_type="purchase", event_ts=datetime(2026, 1, 1, 12, 0)),
        Row(event_id="d", order_id="3", event_type="purchase", event_ts=None),
        Row(event_id="e", order_id="4", event_type="purchase", event_ts=datetime(2026, 1, 1, 12, 2)),
    ]
    df = spark.createDataFrame(rows)
    result = dedup_and_validate(df)
    assert [r.event_id for r in result.collect()] == ["e"]

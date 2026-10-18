"""Unit tests for the Lambda file-tracking logic, using moto to mock
DynamoDB/S3 so no AWS account is needed.

`aws/lambda/file_tracker.py` can't be imported with a normal dotted path
(`lambda` is a reserved keyword), matching how it's actually deployed: zipped
and uploaded standalone, not imported as a package. Loaded here by file path
instead.
"""

import importlib.util
from pathlib import Path

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from moto import mock_aws  # noqa: E402

_module_path = Path(__file__).parent.parent / "aws" / "lambda" / "file_tracker.py"
_spec = importlib.util.spec_from_file_location("file_tracker", _module_path)
file_tracker = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(file_tracker)

is_valid_file = file_tracker.is_valid_file
quarantine_file = file_tracker.quarantine_file
record_delivery = file_tracker.record_delivery


def test_rejects_unsupported_extension():
    valid, reason = is_valid_file("incoming/orders/weird.exe", 100)
    assert not valid
    assert "extension" in reason


def test_rejects_zero_byte_file():
    valid, reason = is_valid_file("incoming/orders/empty.csv", 0)
    assert not valid
    assert "zero-byte" in reason


def test_accepts_valid_csv():
    valid, _ = is_valid_file("incoming/orders/orders_2026-01-01.csv", 2048)
    assert valid


@mock_aws
def test_record_delivery_detects_duplicates():
    table = boto3.resource("dynamodb", region_name="us-east-1").create_table(
        TableName="FileIngestionTracker",
        KeySchema=[
            {"AttributeName": "object_key", "KeyType": "HASH"},
            {"AttributeName": "etag", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "object_key", "AttributeType": "S"},
            {"AttributeName": "etag", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    first = record_delivery(table, "bucket", "incoming/orders/a.csv", "etag-1", 1024)
    duplicate = record_delivery(table, "bucket", "incoming/orders/a.csv", "etag-1", 1024)
    different_version = record_delivery(table, "bucket", "incoming/orders/a.csv", "etag-2", 1024)

    assert first is True
    assert duplicate is False
    assert different_version is True


@mock_aws
def test_quarantine_file_copies_to_quarantine_prefix():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(Bucket="bucket", Key="incoming/orders/empty.csv", Body=b"")

    quarantine_file("bucket", "incoming/orders/empty.csv", "zero-byte file")

    objects = s3.list_objects_v2(Bucket="bucket", Prefix="quarantine/")
    keys = [o["Key"] for o in objects.get("Contents", [])]
    assert "quarantine/empty.csv" in keys

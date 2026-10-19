"""Unit tests for the Lambda file-tracking logic, using moto to mock
DynamoDB/S3 so no AWS account is needed.

`aws/lambda/file_tracker.py` can't be imported with a normal dotted path
(`lambda` is a reserved keyword), matching how it's actually deployed: zipped
and uploaded standalone, not imported as a package. Loaded here by file path
instead.
"""

import importlib.util
import os
from pathlib import Path

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from moto import mock_aws  # noqa: E402

# file_tracker.py's module-level boto3 clients/resources pick up region and
# credentials from the ambient environment at import time; pin fake, fixed
# values so tests are deterministic regardless of what AWS profile/region is
# configured on the machine running them (moto's own recommended practice).
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

_module_path = Path(__file__).parent.parent / "aws" / "lambda" / "file_tracker.py"
_spec = importlib.util.spec_from_file_location("file_tracker", _module_path)
file_tracker = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(file_tracker)

is_valid_file = file_tracker.is_valid_file
classify_file = file_tracker.classify_file
quarantine_file = file_tracker.quarantine_file
move_to_destination = file_tracker.move_to_destination
record_delivery = file_tracker.record_delivery


def test_rejects_unsupported_extension():
    valid, reason = is_valid_file("incoming/ingest/weird.exe", 100)
    assert not valid
    assert "extension" in reason


def test_rejects_zero_byte_file():
    valid, reason = is_valid_file("incoming/ingest/empty.csv", 0)
    assert not valid
    assert "zero-byte" in reason


def test_accepts_valid_csv():
    valid, _ = is_valid_file("incoming/ingest/batch_2026-01-01.csv", 2048)
    assert valid


@mock_aws
def test_classify_file_detects_orders():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(
        Bucket="bucket",
        Key="incoming/ingest/a.csv",
        Body=b"order_id,customer_id,order_ts,status,amount\n1,1,2026-01-01T00:00:00,paid,9.99\n",
    )

    assert classify_file("bucket", "incoming/ingest/a.csv") == "orders"


@mock_aws
def test_classify_file_detects_customers():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(
        Bucket="bucket",
        Key="incoming/ingest/b.csv",
        Body=b"customer_id,first_name,last_name,email,phone,signup_ts\n1,Jane,Doe,jane@example.com,555-0100,2026-01-01T00:00:00\n",
    )

    assert classify_file("bucket", "incoming/ingest/b.csv") == "customers"


@mock_aws
def test_classify_file_returns_none_for_unrecognized_content():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(Bucket="bucket", Key="incoming/ingest/c.csv", Body=b"foo,bar\n1,2\n")

    assert classify_file("bucket", "incoming/ingest/c.csv") is None


@mock_aws
def test_move_to_destination_copies_and_deletes_original():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(Bucket="bucket", Key="incoming/ingest/a.csv", Body=b"data")

    destination_key = move_to_destination("bucket", "incoming/ingest/a.csv", "incoming/orders/")

    assert destination_key == "incoming/orders/a.csv"
    objects = s3.list_objects_v2(Bucket="bucket")
    keys = [o["Key"] for o in objects.get("Contents", [])]
    assert keys == ["incoming/orders/a.csv"]


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

    first = record_delivery(table, "bucket", "incoming/ingest/a.csv", "etag-1", 1024)
    duplicate = record_delivery(table, "bucket", "incoming/ingest/a.csv", "etag-1", 1024)
    different_version = record_delivery(table, "bucket", "incoming/ingest/a.csv", "etag-2", 1024)

    assert first is True
    assert duplicate is False
    assert different_version is True


@mock_aws
def test_quarantine_file_copies_to_quarantine_prefix_and_deletes_original():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    s3.put_object(Bucket="bucket", Key="incoming/ingest/empty.csv", Body=b"")

    quarantine_file("bucket", "incoming/ingest/empty.csv", "zero-byte file")

    objects = s3.list_objects_v2(Bucket="bucket")
    keys = [o["Key"] for o in objects.get("Contents", [])]
    assert keys == ["quarantine/empty.csv"]


@mock_aws
def test_handler_routes_valid_orders_file_to_orders_prefix():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
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
    body = b"order_id,customer_id,order_ts,status,amount\n1,1,2026-01-01T00:00:00,paid,9.99\n"
    s3.put_object(Bucket="bucket", Key="incoming/ingest/orders_2026-01-01.csv", Body=body)

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "bucket"},
                    "object": {
                        "key": "incoming/ingest/orders_2026-01-01.csv",
                        "eTag": "etag-1",
                        "size": len(body),
                    },
                }
            }
        ]
    }

    result = file_tracker.handler(event, None)

    assert result["statusCode"] == 200
    objects = s3.list_objects_v2(Bucket="bucket")
    keys = [o["Key"] for o in objects.get("Contents", [])]
    assert keys == ["incoming/orders/orders_2026-01-01.csv"]


@mock_aws
def test_handler_ignores_duplicate_delivery_without_reaccessing_moved_file():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="bucket")
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
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
    body = b"order_id,customer_id,order_ts,status,amount\n1,1,2026-01-01T00:00:00,paid,9.99\n"
    s3.put_object(Bucket="bucket", Key="incoming/ingest/orders_2026-01-01.csv", Body=body)

    record = {
        "s3": {
            "bucket": {"name": "bucket"},
            "object": {
                "key": "incoming/ingest/orders_2026-01-01.csv",
                "eTag": "etag-1",
                "size": len(body),
            },
        }
    }

    first = file_tracker.handler({"Records": [record]}, None)
    # Redelivery of the same event: the file is already gone from ingest/,
    # but the dedup check must happen before any S3 access, so this must
    # not error trying to re-read a key that no longer exists there.
    second = file_tracker.handler({"Records": [record]}, None)

    assert json_body(first)[0]["status"] == "routed"
    assert json_body(second)[0]["status"] == "duplicate"


def json_body(result):
    import json

    return json.loads(result["body"])

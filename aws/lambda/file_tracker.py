"""Lambda triggered by S3 ObjectCreated events on incoming/ingest/.

Ingest is a staging prefix: every uploaded file lands there first, then this
Lambda validates it, classifies it as orders or customers data by peeking at
its header/content, and moves it to incoming/orders/ or incoming/customers/
(or to quarantine/ if it fails validation or can't be classified). Snowpipe's
own S3 notification (configured separately, see aws/s3_setup.md) watches
incoming/orders/ directly, so it only ever sees files this Lambda has already
validated and routed - the two notifications stay on disjoint prefixes
(ingest/ vs. orders/), which avoids S3's "overlapping rules" restriction on
notification configurations for the same event type.

DynamoDB table `FileIngestionTracker` (partition key `object_key`, sort key
`etag`) is the source of truth this function reads/writes; see
infrastructure/provision_aws.yml for how it's provisioned. Tracking keys off
the original ingest/ key, not the final destination, so it answers "did this
file ever arrive" independent of what happened to it afterward.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ.get("TRACKER_TABLE_NAME", "FileIngestionTracker")
QUARANTINE_PREFIX = os.environ.get("QUARANTINE_PREFIX", "quarantine/")
ORDERS_PREFIX = os.environ.get("ORDERS_PREFIX", "incoming/orders/")
CUSTOMERS_PREFIX = os.environ.get("CUSTOMERS_PREFIX", "incoming/customers/")
ALLOWED_EXTENSIONS = (".csv", ".json")

ORDER_MARKERS = ("order_id", "order_ts")
CUSTOMER_MARKERS = ("first_name", "last_name", "email")


def is_valid_file(key: str, size_bytes: int) -> tuple[bool, str]:
    if not key.lower().endswith(ALLOWED_EXTENSIONS):
        return False, f"unsupported extension for {key}"
    if size_bytes == 0:
        return False, f"zero-byte file: {key}"
    return True, ""


def classify_file(bucket: str, key: str) -> str | None:
    """Peek at the file's header (CSV) or content (JSON) and classify it as
    'orders' or 'customers' by column/field names. Returns None if neither
    set of markers is found."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    content = body.decode("utf-8", errors="ignore").lower()
    header = content.splitlines()[0] if content else ""

    for haystack in (header, content):
        if any(marker in haystack for marker in ORDER_MARKERS):
            return "orders"
        if any(marker in haystack for marker in CUSTOMER_MARKERS):
            return "customers"
    return None


def record_delivery(table, bucket: str, key: str, etag: str, size_bytes: int) -> bool:
    """Returns True if this is the first time we've seen this exact object
    version (bucket/key/etag), False if it's a duplicate delivery."""
    try:
        table.put_item(
            Item={
                "object_key": key,
                "etag": etag,
                "bucket": bucket,
                "size_bytes": size_bytes,
                "first_seen_at": datetime.now(timezone.utc).isoformat(),
            },
            ConditionExpression="attribute_not_exists(object_key)",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def quarantine_file(bucket: str, key: str, reason: str) -> None:
    quarantine_key = QUARANTINE_PREFIX + key.split("/")[-1]
    s3.copy_object(
        Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=quarantine_key
    )
    s3.delete_object(Bucket=bucket, Key=key)
    logger.warning("Quarantined %s/%s: %s", bucket, key, reason)


def move_to_destination(bucket: str, key: str, destination_prefix: str) -> str:
    destination_key = destination_prefix + key.split("/")[-1]
    s3.copy_object(
        Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=destination_key
    )
    s3.delete_object(Bucket=bucket, Key=key)
    return destination_key


def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        etag = record["s3"]["object"]["eTag"]
        size_bytes = record["s3"]["object"]["size"]

        # Dedup gate first, on event metadata alone (no S3 read needed): by
        # the time a redelivered event for the same key/etag arrives, the
        # file may already have been moved out of ingest/ by the first
        # invocation, so this must happen before any further S3 access.
        is_new = record_delivery(table, bucket, key, etag, size_bytes)
        if not is_new:
            logger.info("Duplicate delivery for %s/%s (etag=%s), ignoring", bucket, key, etag)
            results.append({"key": key, "status": "duplicate"})
            continue

        valid, reason = is_valid_file(key, size_bytes)
        if not valid:
            quarantine_file(bucket, key, reason)
            results.append({"key": key, "status": "quarantined", "reason": reason})
            continue

        destination = classify_file(bucket, key)
        if destination is None:
            reason = f"unrecognized content: no orders/customers markers found in {key}"
            quarantine_file(bucket, key, reason)
            results.append({"key": key, "status": "quarantined", "reason": reason})
            continue

        destination_prefix = ORDERS_PREFIX if destination == "orders" else CUSTOMERS_PREFIX
        destination_key = move_to_destination(bucket, key, destination_prefix)
        logger.info(
            "Routed %s/%s to %s (%d bytes)", bucket, key, destination_key, size_bytes
        )
        results.append({"key": key, "status": "routed", "destination": destination_key})

    return {"statusCode": 200, "body": json.dumps(results)}

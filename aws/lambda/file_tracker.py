"""Lambda triggered by S3 ObjectCreated events on incoming/.

Runs *alongside* Snowpipe's own S3 notification (configured separately, see
aws/s3_setup.md) to provide the observability Snowpipe doesn't give you on
its own: a durable record of every file delivery, duplicate detection by
ETag (S3 delivers notifications at least once, so redeliveries of the same
object are expected), and quarantining of files that fail basic validation
before they ever reach Snowpipe.

DynamoDB table `FileIngestionTracker` (partition key `object_key`, sort key
`etag`) is the source of truth this function reads/writes; see
infrastructure/provision_aws.yml for how it's provisioned.
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
ALLOWED_EXTENSIONS = (".csv", ".json")


def is_valid_file(key: str, size_bytes: int) -> tuple[bool, str]:
    if not key.lower().endswith(ALLOWED_EXTENSIONS):
        return False, f"unsupported extension for {key}"
    if size_bytes == 0:
        return False, f"zero-byte file: {key}"
    return True, ""


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
    logger.warning("Quarantined %s/%s: %s", bucket, key, reason)


def handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        etag = record["s3"]["object"]["eTag"]
        size_bytes = record["s3"]["object"]["size"]

        valid, reason = is_valid_file(key, size_bytes)
        if not valid:
            quarantine_file(bucket, key, reason)
            results.append({"key": key, "status": "quarantined", "reason": reason})
            continue

        is_new = record_delivery(table, bucket, key, etag, size_bytes)
        if not is_new:
            logger.info("Duplicate delivery for %s/%s (etag=%s), ignoring", bucket, key, etag)
            results.append({"key": key, "status": "duplicate"})
            continue

        logger.info("Tracked new file delivery: %s/%s (%d bytes)", bucket, key, size_bytes)
        results.append({"key": key, "status": "tracked"})

    return {"statusCode": 200, "body": json.dumps(results)}

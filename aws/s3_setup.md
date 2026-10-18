# S3 + Lambda + Snowpipe setup (Pipeline A automation)

Manual steps to automate ingestion once the manual `COPY INTO` path
(`snowflake/02_bronze_ingestion.sql`) is validated.

Two independent S3 event notifications are configured on the same
`incoming/` prefix, each doing a different job:

- **Lambda** (`aws/lambda/file_tracker.py`): fires first, records every file
  delivery in DynamoDB (`FileIngestionTracker`), detects duplicate
  redeliveries by ETag, and quarantines files that fail basic validation
  (wrong extension, zero bytes) before they ever reach Snowflake.
- **Snowpipe**: fires independently via its own SQS-backed notification and
  loads valid files into `RETAIL_DWH.BRONZE.RAW_ORDERS`.

They're decoupled on purpose — Lambda's job is observability/validation, not
gating the load, so a Lambda failure never blocks ingestion; it would just
mean a gap in the delivery-tracking table, visible the same way a stale
Snowpipe load is (`monitoring/freshness_checks.sql`).

## 1. Create the bucket

```
aws s3 mb s3://<your-bucket>-retail-orders
aws s3api put-bucket-notification-configuration ...   # see step 3
```

Layout:
```
<bucket>/
└── incoming/
    ├── orders/
    └── customers/
```

## 2. Deploy the Lambda and wire its S3 trigger

Handled by `infrastructure/provision_aws.yml` (DynamoDB table, execution
role, function deploy, S3 `ObjectCreated` trigger on `incoming/`). To deploy
manually instead:

```
zip aws/lambda/file_tracker.zip aws/lambda/file_tracker.py
aws lambda create-function \
    --function-name retail-file-tracker \
    --runtime python3.12 \
    --handler file_tracker.handler \
    --zip-file fileb://aws/lambda/file_tracker.zip \
    --role arn:aws:iam::<account_id>:role/retail-file-tracker-lambda-role \
    --environment Variables="{TRACKER_TABLE_NAME=FileIngestionTracker,QUARANTINE_PREFIX=quarantine/}"
```

## 3. Storage integration (run in Snowflake)

```sql
CREATE STORAGE INTEGRATION RETAIL_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/snowflake-retail-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket>-retail-orders/incoming/');

DESC INTEGRATION RETAIL_S3_INT;  -- gives you the external ID + Snowflake IAM user ARN
```

Create the IAM role (`infrastructure/` Ansible playbook handles this
idempotently) trusting the Snowflake IAM user ARN returned above, scoped to
read-only access on the bucket prefix.

## 4. Event notification → Snowpipe auto-ingest

Configure the bucket's event notification to deliver `s3:ObjectCreated:*`
events on `incoming/` to the SQS queue Snowflake creates for the pipe
(`SYSTEM$PIPE_STATUS` / `SHOW PIPES` after creating `ORDERS_PIPE` in
`snowflake/02_bronze_ingestion.sql` returns the queue ARN to subscribe).

## Idempotency note

S3 delivers notifications **at least once**. `COPY INTO` is naturally
idempotent per file (Snowflake tracks loaded files for 64 days by default),
but application-level dedup still happens in Silver
(`snowflake/03_silver_transformations.sql`) via `QUALIFY ROW_NUMBER()...= 1`
so a redelivered file never double-counts a row downstream.

## 5. Verify

```
aws s3 cp data/sample_orders.csv s3://<your-bucket>-retail-orders/incoming/orders/
```

Then in Snowflake:
```sql
SELECT SYSTEM$PIPE_STATUS('RETAIL_DWH.BRONZE.ORDERS_PIPE');
SELECT * FROM RETAIL_DWH.BRONZE.RAW_ORDERS ORDER BY INGESTED_AT DESC LIMIT 10;
```

And confirm the Lambda tracked the same delivery:
```
aws dynamodb get-item --table-name FileIngestionTracker \
    --key '{"object_key": {"S": "incoming/orders/sample_orders.csv"}, "etag": {"S": "<etag-from-s3-cp-output>"}}'
```

# S3 + Lambda + Snowpipe setup (Pipeline A automation)

Automate ingestion once the manual `COPY INTO` path
(`snowflake/02_bronze_ingestion.sql`) is validated.

Two independent S3 event notifications end up configured on the same
`incoming/` prefix, each doing a different job:

- **Lambda** (`aws/lambda/file_tracker.py`): records every file delivery in
  DynamoDB (`FileIngestionTracker`), detects duplicate redeliveries by ETag,
  and quarantines files that fail basic validation (wrong extension, zero
  bytes) before they'd otherwise reach Snowflake.
- **Snowpipe**: fires independently via its own SQS-backed notification and
  loads valid files into `RETAIL_DWH.BRONZE.RAW_ORDERS`.

They're decoupled on purpose — Lambda's job is observability/validation, not
gating the load, so a Lambda failure never blocks ingestion; it would just
mean a gap in the delivery-tracking table, visible the same way a stale
Snowpipe load is (`monitoring/freshness_checks.sql`).

## 1. Provision the bucket, Lambda, and DynamoDB table (pass 1)

```
ansible-playbook infrastructure/provision_aws.yml -e bucket_name=<your-bucket>-retail-orders
```

This creates the S3 bucket (with `incoming/orders/` and `incoming/customers/`
prefixes), the `FileIngestionTracker` DynamoDB table, the Lambda's execution
role/function, its S3 trigger, and the `snowflake-retail-role` IAM role used
in step 2 below — with a deny-all placeholder trust policy for now, since
Snowflake hasn't told us who to trust yet.

## 2. Create the storage integration in Snowflake, then tighten the role (pass 2)

```sql
CREATE STORAGE INTEGRATION RETAIL_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/snowflake-retail-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket>-retail-orders/incoming/');

DESC INTEGRATION RETAIL_S3_INT;  -- read STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the output
```

Re-run the playbook with those two values to replace the placeholder trust
policy with the real relationship:

```
ansible-playbook infrastructure/provision_aws.yml \
    -e bucket_name=<your-bucket>-retail-orders \
    -e snowflake_iam_user_arn='<STORAGE_AWS_IAM_USER_ARN>' \
    -e snowflake_external_id='<STORAGE_AWS_EXTERNAL_ID>'
```

## 3. Create the stage and pipe, then wire Snowpipe's S3 notification

Run `snowflake/02_bronze_ingestion.sql`'s stage/pipe statements (uncomment
the `CREATE STAGE`/storage-integration lines, now that the integration
exists), then:

```sql
SHOW PIPES LIKE 'ORDERS_PIPE' IN SCHEMA RETAIL_DWH.BRONZE;
-- read notification_channel (the SQS queue ARN) from the output
```

In the S3 console (or `aws s3api put-bucket-notification-configuration`),
add a second event notification on the same bucket/prefix — `ObjectCreated:*`
on `incoming/` — targeting that SQS queue. This is separate from, and
doesn't replace, the Lambda trigger step 1 already configured.

## Idempotency note

S3 delivers notifications **at least once**. `COPY INTO` is naturally
idempotent per file (Snowflake tracks loaded files for 64 days by default),
and the Lambda dedups by ETag before Snowpipe even sees a redelivered file;
application-level dedup also happens in Silver
(`snowflake/03_silver_transformations.sql`) via `QUALIFY ROW_NUMBER()...= 1`
so a redelivered file never double-counts a row downstream either way.

## 4. Verify

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

# S3 + Lambda + Snowpipe setup (Pipeline A automation)

Automate ingestion once the manual `COPY INTO` path
(`snowflake/02_bronze_ingestion.sql`) is validated.

Three prefixes, each with a distinct job, and two S3 event notifications on
two disjoint prefixes:

- **`incoming/ingest/`** — where files actually get uploaded. The Lambda
  (`aws/lambda/file_tracker.py`) is the only thing watching this prefix. It
  records every delivery in DynamoDB (`FileIngestionTracker`), detects
  duplicate redeliveries by ETag, quarantines files that fail basic
  validation (wrong extension, zero bytes) or that it can't classify, and
  for everything else, peeks at the file's header/content to tell orders
  data from customers data and **moves** it to the matching prefix below.
- **`incoming/orders/`** — only ever written to by the Lambda, never
  uploaded to directly. Snowpipe's own SQS-backed notification watches
  *this* prefix and loads valid files into `RETAIL_DWH.BRONZE.RAW_ORDERS`.
- **`incoming/customers/`** — same idea, for customer data. (No Snowpipe on
  this prefix yet; only `RAW_ORDERS` has an auto-ingest pipe today. Loading
  `RAW_CUSTOMERS` automatically would mean adding a parallel
  `CUSTOMERS_PIPE`/`CUSTOMERS_STAGE`, not built here.)

Keeping Lambda's notification scoped to `incoming/ingest/` and Snowpipe's
scoped to `incoming/orders/` means the two notifications never overlap —
important because S3 rejects overlapping prefix/event-type rules even
across different destination types (Lambda vs. SQS), so a single shared
`incoming/` prefix for both doesn't work. It also means the Lambda never
re-triggers itself when it writes the moved file into `incoming/orders/` or
`incoming/customers/`.

They're still decoupled where it matters — Snowpipe only ever sees files
the Lambda has already validated and classified, so a Lambda bug or outage
just stops files moving out of `incoming/ingest/` (visible as stuck/missing
files there), rather than corrupting what lands in Bronze.

## 1. Provision the bucket, Lambda, and DynamoDB table (pass 1)

```
ansible-playbook infrastructure/provision_aws.yml -e bucket_name=<your-bucket>-retail-orders
```

This creates the S3 bucket (with `incoming/ingest/`, `incoming/orders/`, and
`incoming/customers/` prefixes), the `FileIngestionTracker` DynamoDB table,
the Lambda's execution role/function, its S3 trigger on `incoming/ingest/`,
and the `snowflake-retail-role` IAM role used in step 2 below — with a
deny-all placeholder trust policy for now, since Snowflake hasn't told us
who to trust yet.

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

## 3. Create the stage and pipe, then wire Snowpipe's S3 notification (pass 3)

Run `snowflake/02_bronze_ingestion.sql`'s stage/pipe statements (uncomment
the `CREATE STAGE`/storage-integration lines, now that the integration
exists). The stage URL points at `incoming/orders/` specifically, not
`incoming/` — Snowpipe should only ever see files the Lambda has already
routed there. Then:

```sql
SHOW PIPES LIKE 'ORDERS_PIPE' IN SCHEMA RETAIL_DWH.BRONZE;
-- read notification_channel (the SQS queue ARN) from the output
```

Re-run the playbook a third time with that value to wire the bucket's
`incoming/orders/` notification to Snowpipe's queue:

```
ansible-playbook infrastructure/provision_aws.yml \
    -e bucket_name=<your-bucket>-retail-orders \
    -e snowflake_iam_user_arn='<STORAGE_AWS_IAM_USER_ARN>' \
    -e snowflake_external_id='<STORAGE_AWS_EXTERNAL_ID>' \
    -e snowflake_sqs_queue_arn='<notification_channel from SHOW PIPES>'
```

(Keep passing the pass-2 values too — omitting them would revert the
Snowflake trust policy back to its deny-all placeholder.) No extra IAM
permissions are needed for this task — Snowflake's auto-provisioned SQS
queue already has a resource policy permitting S3 to send it messages, set
up automatically when the storage integration was created. The
`incoming/orders/` prefix here stays disjoint from the Lambda's
`incoming/ingest/` notification, so the two never hit S3's
overlapping-rules restriction (see above).

If a file lands directly in `incoming/orders/` (bypassing the Lambda
entirely, which shouldn't normally happen — uploads are meant to go to
`incoming/ingest/`) *before* this notification exists, Snowpipe never hears
about it (S3 notifications only fire on new writes, not retroactively).
Check for this with
`SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME =>
'RETAIL_DWH.BRONZE.RAW_ORDERS', START_TIME => DATEADD(hours, -2,
CURRENT_TIMESTAMP())));` — if it's empty, the pipe was never notified.
Backfill with `ALTER PIPE RETAIL_DWH.BRONZE.ORDERS_PIPE REFRESH;`, which
scans the stage directly and bypasses the notification mechanism.

## Idempotency note

S3 delivers notifications **at least once**. The Lambda's dedup check (by
ETag) happens before it ever reads or moves the file, so a redelivered
ingest/ event is a no-op rather than a second copy/move attempt. `COPY INTO`
is naturally idempotent per file too (Snowflake tracks loaded files for 64
days by default); application-level dedup also happens in Silver
(`snowflake/03_silver_transformations.sql`) via `QUALIFY ROW_NUMBER()...= 1`
so a redelivered file never double-counts a row downstream either way.

## 4. Verify

```
aws s3 cp data/sample_orders.csv s3://<your-bucket>-retail-orders/incoming/ingest/
```

The Lambda should pick this up, recognize it as orders data, and move it to
`incoming/orders/sample_orders.csv` within a few seconds. Then in Snowflake:
```sql
SELECT SYSTEM$PIPE_STATUS('RETAIL_DWH.BRONZE.ORDERS_PIPE');
SELECT * FROM RETAIL_DWH.BRONZE.RAW_ORDERS ORDER BY INGESTED_AT DESC LIMIT 10;
```

And confirm the Lambda tracked the delivery (keyed by the original
`incoming/ingest/` path, not where the file ended up):
```
aws dynamodb get-item --table-name FileIngestionTracker \
    --key '{"object_key": {"S": "incoming/ingest/sample_orders.csv"}, "etag": {"S": "<etag-from-s3-cp-output>"}}'
```

# S3 + Lambda + Snowpipe setup (Pipeline A automation)

Automate ingestion once the manual `COPY INTO` path
(`snowflake/02_bronze_ingestion.sql`) is validated.

Three prefixes, each with a distinct job, and three S3 event notifications
on three disjoint prefixes:

- **`incoming/ingest/`** — where files actually get uploaded. The Lambda
  (`aws/lambda/file_tracker.py`) is the only thing watching this prefix. It
  records every delivery in DynamoDB (`FileIngestionTracker`), detects
  duplicate redeliveries by ETag, quarantines files that fail basic
  validation (wrong extension, zero bytes) or that it can't classify, and
  for everything else, peeks at the file's header/content to tell orders
  data from customers data and **moves** it to the matching prefix below.
- **`incoming/orders/`** — only ever written to by the Lambda, never
  uploaded to directly. `ORDERS_PIPE`'s own SQS-backed notification watches
  *this* prefix and loads valid files into `RETAIL_DWH.BRONZE.RAW_ORDERS`.
- **`incoming/customers/`** — same idea, via `CUSTOMERS_PIPE`, loading into
  `RETAIL_DWH.BRONZE.RAW_CUSTOMERS`.

Keeping each notification on its own disjoint prefix (Lambda on
`incoming/ingest/`, the two Snowpipe queues on `incoming/orders/` and
`incoming/customers/`) means none of them ever overlap — important because
S3 rejects overlapping prefix/event-type rules even across different
destination types (Lambda vs. SQS), so a single shared `incoming/` prefix
for all three doesn't work. It also means the Lambda never re-triggers
itself when it writes the moved file into `incoming/orders/` or
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

## 3. Create the stages/pipes, then wire Snowpipe's S3 notifications (pass 3)

Run `snowflake/02_bronze_ingestion.sql`'s stage/pipe statements (uncomment
the `CREATE STAGE`/`CREATE PIPE` lines for both `ORDERS_STAGE`/`ORDERS_PIPE`
and `CUSTOMERS_STAGE`/`CUSTOMERS_PIPE`, now that the integration exists).
Each stage URL points at its own prefix (`incoming/orders/` or
`incoming/customers/`) specifically, not `incoming/` — each pipe should
only ever see files the Lambda has already routed there. Then:

```sql
SHOW PIPES IN SCHEMA RETAIL_DWH.BRONZE;
-- read notification_channel (the SQS queue ARN) for each pipe from the output
```

Re-run the playbook a third time with those values to wire the bucket's
`incoming/orders/` and `incoming/customers/` notifications to each queue
(omit either `-e snowflake_*_sqs_queue_arn` if you only built one of the
two pipes):

```
ansible-playbook infrastructure/provision_aws.yml \
    -e bucket_name=<your-bucket>-retail-orders \
    -e snowflake_iam_user_arn='<STORAGE_AWS_IAM_USER_ARN>' \
    -e snowflake_external_id='<STORAGE_AWS_EXTERNAL_ID>' \
    -e snowflake_orders_sqs_queue_arn='<ORDERS_PIPE notification_channel>' \
    -e snowflake_customers_sqs_queue_arn='<CUSTOMERS_PIPE notification_channel>'
```

(Keep passing the pass-2 values too — omitting them would revert the
Snowflake trust policy back to its deny-all placeholder.) No extra IAM
permissions are needed for this task — Snowflake's auto-provisioned SQS
queues already have resource policies permitting S3 to send them messages,
set up automatically when the storage integration was created. Each prefix
here stays disjoint from the Lambda's `incoming/ingest/` notification and
from each other, so none of them hit S3's overlapping-rules restriction
(see above).

If a file lands directly in `incoming/orders/` or `incoming/customers/`
(bypassing the Lambda entirely, which shouldn't normally happen — uploads
are meant to go to `incoming/ingest/`) *before* the matching notification
exists, Snowpipe never hears about it (S3 notifications only fire on new
writes, not retroactively). Check for this with
`SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME =>
'RETAIL_DWH.BRONZE.RAW_ORDERS', START_TIME => DATEADD(hours, -2,
CURRENT_TIMESTAMP())));` (swap in `RAW_CUSTOMERS` for the other pipe) — if
it's empty, the pipe was never notified. Backfill with
`ALTER PIPE RETAIL_DWH.BRONZE.ORDERS_PIPE REFRESH;` (or `CUSTOMERS_PIPE`),
which scans the stage directly and bypasses the notification mechanism.

## Idempotency note

S3 delivers notifications **at least once**. The Lambda's dedup check (by
ETag) happens before it ever reads or moves the file, so a redelivered
ingest/ event is a no-op rather than a second copy/move attempt. `COPY INTO`
is naturally idempotent per file too (Snowflake tracks loaded files for 64
days by default); application-level dedup also happens in Silver
(`snowflake/03_silver_transformations.sql`) via `QUALIFY ROW_NUMBER()...= 1`
so a redelivered file never double-counts a row downstream either way.

**Gotcha when manually re-testing**: dedup is keyed on `(object_key, etag)`,
not on "does this file currently exist in its destination." If you delete a
file from `incoming/orders/`/`incoming/customers/` after it's already been
routed there, then re-drop the *identical* file (same bytes, same ETag)
into `incoming/ingest/` to test again, the Lambda will recognize it as
already-seen and silently skip it - it won't get reprocessed or re-moved.
You'll see the file just sit in `incoming/ingest/` with no error anywhere.
To force a fresh test, either change the file's content slightly (anything
that changes its ETag, e.g. regenerate sample data) or delete its
DynamoDB tracking item (`object_key`/`etag` from
`aws dynamodb get-item ...`) before re-uploading.

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

Same for customers data — upload `data/sample_customers.csv` to
`incoming/ingest/` and it should get classified and routed to
`incoming/customers/`, then `CUSTOMERS_PIPE` loads it into
`RETAIL_DWH.BRONZE.RAW_CUSTOMERS`:
```sql
SELECT SYSTEM$PIPE_STATUS('RETAIL_DWH.BRONZE.CUSTOMERS_PIPE');
SELECT * FROM RETAIL_DWH.BRONZE.RAW_CUSTOMERS ORDER BY INGESTED_AT DESC LIMIT 10;
```

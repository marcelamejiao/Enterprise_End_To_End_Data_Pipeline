# Incident Runbook

## Snowpipe backlog / stuck pipe

**Symptom:** `monitoring/freshness_checks.sql` → `FRESHNESS_CHECK.IS_STALE = TRUE`, or `SYSTEM$PIPE_STATUS` shows a growing `pendingFileCount`.

1. `SELECT SYSTEM$PIPE_STATUS('RETAIL_DWH.BRONZE.ORDERS_PIPE');` — check `executionState` and `lastIngestedFilePath`.
2. Check `RETAIL_DWH.BRONZE.ORDER_LOAD_QUARANTINE` and the pipe's `COPY_HISTORY` for recent errors (malformed rows, schema drift).
3. If the pipe is paused: `ALTER PIPE RETAIL_DWH.BRONZE.ORDERS_PIPE REFRESH;`
4. If files are stuck due to a schema change, fix the file format/table definition, reprocess the specific files via manual `COPY INTO ... FILES = (...)`, then confirm `VOLUME_ANOMALY_CHECK` returns no anomaly for that date.

## Streaming job (Spark consumer) down or lagging

**Symptom:** Kafka consumer group lag growing, or no new rows in Delta/Snowflake output.

1. Check the Spark job logs for the last committed offset / exception.
2. Restart `spark_consumer.py` with the **same `--checkpoint-path`** — it resumes from the last committed Kafka offset; do not delete the checkpoint directory unless you intend to reprocess from `earliest` and accept re-processing duplicates (dedup logic will still drop event_id repeats within the watermark window).
3. If the lag is due to a schema change in the producer, fix `EVENT_SCHEMA` in `streaming/spark_consumer.py` to match, redeploy, then restart.

## Failed/long-running pipeline run

**Symptom:** A row appears in `RETAIL_DWH.SILVER.FAILED_RUNS`.

1. Identify the pipeline from `PIPELINE_NAME` and check its logs (Task history for Snowflake-native tasks: `SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = '<task>';`).
2. For transient failures (warehouse suspended, network blip), re-run — pipeline steps are designed to be idempotent (COPY INTO tracks loaded files, Silver dedups on latest `INGESTED_AT`).
3. For persistent failures, open a GitHub Issue tagged `incident`, link the relevant run-log row, and note root cause once found.

## Masking policy not applying

**Symptom:** A non-privileged role sees unmasked email/phone.

1. Confirm the querying role is not `PII_ACCESS_ROLE` and does not inherit it transitively (`SHOW GRANTS OF ROLE PII_ACCESS_ROLE;`).
2. Confirm the policy is actually attached: `SELECT POLICY_NAME FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(REF_ENTITY_NAME => 'RETAIL_DWH.BRONZE.RAW_CUSTOMERS', REF_ENTITY_DOMAIN => 'TABLE'));`
3. Re-apply from `snowflake/06_privacy_policies.sql` if missing, and treat this as a priority incident since it's a PII exposure — document in a GitHub Issue regardless of how quickly it's fixed.

## Alerting itself goes silent

**Symptom:** No alerts for a known-bad period.

1. Check `monitoring/alerting.py` is still scheduled/running (its own run log, separate from pipeline run logs, should exist in a real deployment — a monitor that only runs when pipelines run can't catch "pipeline stopped running entirely").
2. Verify `ALERT_WEBHOOK_URL` is still valid.

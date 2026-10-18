# Enterprise End-to-End Data Pipeline

Portfolio project demonstrating secure batch and streaming data pipelines, data quality, governance, monitoring, and CI/CD on a retail orders use case.

See [PLAN.md](PLAN.md) for the full build plan.

## Architecture

- **Pipeline A — Batch ingestion**: S3 → (Lambda file-tracking + Snowpipe, independently) → Snowflake Bronze/Silver/Gold (medallion architecture) using SQL transformations, Dynamic Tables, and Streams/Tasks for CDC. The Lambda tracks every file delivery in DynamoDB, flags duplicates, and quarantines invalid files before Snowpipe loads them.
- **Pipeline B — Real-time event processing**: Kafka → Spark Structured Streaming, with checkpointing, watermarking, and dedup, writing to Delta Lake or Snowflake.
- **Pipeline C — Governance & operational monitoring**: PII masking, role-based access, pipeline run logs, freshness checks, alerting, and CI/CD via GitHub Actions.

Details in [docs/architecture.md](docs/architecture.md) (added as the project is built out).

## Repo layout

```
.
├── data/              # synthetic data generators
├── aws/               # S3 + Lambda setup
├── snowflake/         # DDL, transformations, CDC, privacy policies
├── streaming/          # Kafka producer + Spark consumer
├── tests/             # pytest suite
├── monitoring/        # run logs, freshness checks, alerting
├── infrastructure/    # Ansible playbooks
├── docs/              # architecture, data dictionary, incident runbook
└── .github/workflows/ # CI/CD
```

## Getting started

Steps are numbered in the order they actually need to happen — later steps
depend on earlier ones. AWS-only steps (section 3) are optional: skip them
and Pipeline A still works end-to-end with the manual load from section 2.

### 1. Local setup

1. `pip install -r requirements.txt`
2. `python data/generate_sample_data.py` — writes `data/sample_customers.csv`, `sample_orders.csv`, `sample_order_events.csv`

### 2. Pipeline A — Snowflake core (no AWS needed)

3. In a Snowflake worksheet (or `snowsql`), run in order: `snowflake/01_database_setup.sql`, then **Part 1 only** of `snowflake/02_bronze_ingestion.sql` (Part 2, Snowpipe, needs section 3 below first).
4. Load the sample files into each table's stage, then run Part 1's `COPY INTO` statements:
   ```
   snowsql -q "PUT file://data/sample_orders.csv @RETAIL_DWH.BRONZE.%RAW_ORDERS"
   snowsql -q "PUT file://data/sample_customers.csv @RETAIL_DWH.BRONZE.%RAW_CUSTOMERS"
   ```
5. Run `snowflake/03_silver_transformations.sql`, `04_gold_metrics.sql`, `05_streams_tasks.sql`, `06_privacy_policies.sql`, in order.
6. Verify: `SELECT * FROM RETAIL_DWH.GOLD.DAILY_SALES;` should return aggregated rows.

### 3. Pipeline A — AWS automation (optional, needs an AWS account)

7. Follow [aws/s3_setup.md](aws/s3_setup.md) steps 1–4: provision the bucket/Lambda/DynamoDB table, create the Snowflake storage integration (note: this is a two-pass IAM trust setup — the doc explains why), create the stage/pipe (Part 2 of `02_bronze_ingestion.sql`), and wire Snowpipe's own S3 notification.
8. Verify by uploading a file and checking both Snowflake and DynamoDB, per step 4 in that doc.

### 4. Pipeline B — Streaming

9. Follow [streaming/README.md](streaming/README.md): start Kafka via `docker compose`, run the producer, run the Spark consumer.

### 5. Pipeline C — Governance & monitoring

10. Run `monitoring/freshness_checks.sql` against your Snowflake account to create the freshness/anomaly views used in step 12.
11. Confirm masking works: query `RAW_CUSTOMERS.EMAIL` as a role other than `PII_ACCESS_ROLE` (should show `***MASKED***`), then as `PII_ACCESS_ROLE` (should show the real value).
12. Set `SNOWFLAKE_ACCOUNT`/`SNOWFLAKE_USER`/`SNOWFLAKE_PASSWORD`/`SNOWFLAKE_WAREHOUSE`/`ALERT_WEBHOOK_URL` env vars and run `python monitoring/alerting.py` (or schedule it) to exercise the alert path.

### 6. CI/CD (GitHub)

13. In the GitHub repo's Settings → Secrets, add: `SNOWFLAKE_ACCOUNT`/`USER`/`PASSWORD`/`WAREHOUSE`/`DEPLOY_ROLE`, `AWS_ACCESS_KEY_ID`/`SECRET_ACCESS_KEY`/`DEFAULT_REGION`, `SNOWFLAKE_IAM_USER_ARN`/`EXTERNAL_ID`, `ALERT_WEBHOOK_URL`.
14. Open a PR touching `snowflake/`, `streaming/`, or Python files to trigger `lint-and-test.yml` / `streaming-ci.yml`. Merging to `main` with `snowflake/**` changes triggers `deploy-snowflake.yml` automatically. Run `deploy-infra.yml` manually (`workflow_dispatch`) only when you intend to (re)provision AWS — it never runs on its own, to avoid surprise cloud cost.

### Tests

`pytest tests/` — Snowflake-backed tests self-skip without credentials; streaming dedup and Lambda file-tracker tests run fully locally (`local[2]` Spark, `moto`-mocked AWS), no account needed for those.

## Status

All three pipelines scaffolded end-to-end: Pipeline A (Snowflake medallion architecture + Snowpipe), Pipeline B (Kafka + Spark Structured Streaming with checkpointing/dedup), and Pipeline C (masking, RBAC, monitoring, alerting, Ansible, GitHub Actions CI/CD). See [PLAN.md](PLAN.md) for the full build plan and [docs/architecture.md](docs/architecture.md) for the detailed design.

# Enterprise End-to-End Data Pipeline — Portfolio Project Plan

## Context

This repo demonstrates secure batch and streaming data pipelines, data quality, governance, monitoring, and CI/CD, built to connect existing SQL/Python/ETL/AWS/Ansible skills with Snowflake, Kafka/Kinesis, and Spark — the stack targeted by the job description this project is based on.

## Approach

Build three pipelines in sequence, each de-risking the next, using a retail orders use case throughout (customers, orders, order events) since it's concrete and easy to explain to interviewers.

### 0. Repo setup
- Git repo, `.gitignore` for Python/Snowflake creds/`.env`/Ansible state/`__pycache__`/checkpoints.
- Folder structure: `data/`, `aws/` (+ `lambda/`), `snowflake/`, `streaming/`, `tests/`, `monitoring/`, `infrastructure/`, `docs/`, `.github/workflows/`, `README.md`.
- GitHub Issues/Projects board as the Jira stand-in for collaboration artifacts.

### Phase 1 — Pipeline A: Batch ingestion (build first)
- `data/`: Python generator (Faker) for synthetic `customers`, `orders`, `order_events` CSV/JSON.
- `snowflake/01_database_setup.sql` through `06_privacy_policies.sql`, in order:
  1. Database/schema setup (`RETAIL_DWH` with BRONZE/SILVER/GOLD)
  2. Bronze ingestion (COPY INTO `RAW_ORDERS`)
  3. Silver transformations (`CLEAN_ORDERS` dedup view)
  4. Gold metrics (`DAILY_SALES` Dynamic Table)
  5. Streams & Tasks CDC variant (`ORDER_CHANGES` stream + `PROCESS_ORDER_CHANGES` task)
  6. Privacy policies (email/phone masking policy)
- `aws/s3_setup.md`: S3 bucket with `incoming/` prefix, storage integration, Snowpipe auto-ingest config.
- `tests/`: pytest + `snowflake-connector-python` assertions (row counts, nulls, uniqueness, referential integrity).
- Demo path: upload CSV to S3 → Snowpipe lands in Bronze → Dynamic Table refreshes Gold → query revenue-by-day.

### Phase 2 — Pipeline B: Streaming
- `streaming/`: Kafka via `docker-compose` (local, to avoid cloud streaming cost) producing synthetic purchase/payment events (`event_id`, `order_id`, `event_ts`).
- `streaming/spark_consumer.py`: Spark Structured Streaming — checkpointing, watermarking for late events, dedup on `event_id`, writes to Delta Lake (S3/local) or directly into a Snowflake Bronze table.
- Kinesis swap-in treated as optional/stretch, added only after the Kafka version works end-to-end.
- Demo path: start producer → consumer processes events with checkpointing/dedup → results land in Silver.

### Phase 3 — Pipeline C: Governance & operational excellence
- Finish `snowflake/06_privacy_policies.sql`: role separation (`INGEST_ROLE`, `TRANSFORM_ROLE`, `ANALYST_ROLE`, `PII_ACCESS_ROLE`) plus masking policy application.
- `monitoring/`: pipeline run-log table (name, start/end, rows processed/rejected, status), freshness-check query, failure/staleness alerting (Task → webhook).
- `infrastructure/`: Ansible playbook(s) to provision S3 bucket, IAM role, and Snowflake objects idempotently.
- `docs/architecture.md`, `docs/data_dictionary.md` (owners, sensitivity, retention), `docs/incident_runbook.md`.

### CI/CD (`.github/workflows/`)
| Workflow | Trigger | Purpose |
|---|---|---|
| `lint-and-test.yml` | PR to `main` | `sqlfluff` on `snowflake/`, `pytest` on `tests/`, `ruff`/`black --check` on Python |
| `deploy-snowflake.yml` | push to `main`, path-filtered on `snowflake/**` | Apply SQL in order via SnowSQL CLI or `schemachange`, using GitHub Secrets |
| `deploy-infra.yml` | manual `workflow_dispatch` only | Run Ansible playbook against AWS (never auto, to control cost) |
| `streaming-ci.yml` | PR touching `streaming/**` | Spin up Kafka via docker-compose in the runner, run Spark job against a fixture, assert output |

Branch protection on `main`, feature branches + PRs for every change, so the git history itself demonstrates PR-based review.

### Build order
1. Repo + folder scaffold, pushed with README stub.
2. Pipeline A fully working locally (manual COPY INTO) before automating with Snowpipe/S3.
3. Snowpipe/S3 automation on top of a working Pipeline A.
4. Pipeline B with local Kafka + Spark.
5. Pipeline C wraps both with governance, monitoring, Ansible, GitHub Actions.
6. Final pass: architecture diagram, data dictionary, incident runbook, polished README.

### Cost control
- Snowflake free trial; `AUTO_SUSPEND` on warehouses; loose `TARGET_LAG` on Dynamic Tables.
- Kafka stays local via Docker; Kinesis only stood up temporarily if wanted for the resume line, then torn down.
- `deploy-infra.yml` gated to manual dispatch so Actions never silently incurs AWS charges on push.

## Critical files
- `snowflake/01_database_setup.sql` … `06_privacy_policies.sql` — core warehouse logic
- `streaming/spark_consumer.py` and a Kafka producer script
- `data/generate_sample_data.py` — Faker-based synthetic data generator
- `.github/workflows/lint-and-test.yml`, `deploy-snowflake.yml`, `deploy-infra.yml`, `streaming-ci.yml`
- `infrastructure/` Ansible playbook(s)
- `docs/architecture.md`, `docs/data_dictionary.md`, `docs/incident_runbook.md`
- `README.md`

## Verification
- Phase 1: run the data generator, manually `COPY INTO` a sample file, confirm Silver view dedups correctly, confirm Gold Dynamic Table reflects aggregates, confirm `pytest tests/` passes against the loaded data.
- Phase 2: run `docker-compose up` for Kafka, run the producer, run the Spark consumer, confirm deduped/watermarked output lands in Delta/Snowflake, confirm checkpoint survives a consumer restart.
- Phase 3: trigger a masking policy test (query as `PII_ACCESS_ROLE` vs. another role), confirm run-log rows populate, confirm GitHub Actions workflows go green on a test PR, run `deploy-infra.yml` manually once against a scratch AWS account.
- End-to-end: full demo walkthrough (upload file → Snowpipe → Gold; Kafka event → Spark → Silver) documented in the README.

enterprise-data-pipeline/
├── data/
│   └── sample_orders.csv
├── aws/
│   ├── s3_setup.md
│   └── lambda/
├── snowflake/
│   ├── 01_database_setup.sql
│   ├── 02_bronze_ingestion.sql
│   ├── 03_silver_transformations.sql
│   ├── 04_gold_metrics.sql
│   ├── 05_streams_tasks.sql
│   └── 06_privacy_policies.sql
├── streaming/
│   └── spark_consumer.py
├── tests/
├── monitoring/
├── infrastructure/
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── incident_runbook.md
└── README.md



ortfolio project: Enterprise Data Pipeline Engineering
Based on this job description, I recommend building an end-to-end data engineering project that demonstrates secure batch and streaming pipelines, data quality, governance, monitoring, and CI/CD.
Since you're developing your Snowflake skills and have experience with SQL, Python, ETL tools, PostgreSQL, AWS, and Ansible, you can build a portfolio project that connects those existing skills with the technologies requested in the role. 
1. Project architecture
Data sources
PostgreSQL · MongoDB · Application events · CSV/JSON
Batch pipeline
AWS S3 → Lambda / Snowpipe
Streaming pipeline
Kafka or Kinesis → Spark
Data lake and warehouse
S3 + Delta Lake for lakehouse processing, and/or Snowflake for analytics
Medallion architecture
Bronze
Raw data
Silver
Clean data
Gold
Business metrics
Business consumption
Power BI · SQL analytics · Data science
Cross-cutting controls: IAM, encryption, data masking, data quality, observability, CI/CD and audit logs.
You don't need to deploy every technology simultaneously. Build one working pipeline first, then add integrations to demonstrate breadth without creating an unnecessarily expensive or complicated project.
2. Three pipelines to implement


Pipeline A — Batch ingestion
Start here
Ingest customer, orders and transaction CSV/JSON files from S3 into Snowflake. Use Snowpipe for automated ingestion, SQL transformations for Silver and Gold layers, and Streams and Tasks or Dynamic Tables for incremental processing.
Skills: AWS S3, Snowflake, SQL, Python, ETL, data quality.


Pipeline B — Real-time event processing
Capture simulated purchase and payment events with Kafka or Kinesis. Process events using Spark Structured Streaming, validate records, handle duplicates and late events, and write results to S3/Delta Lake or Snowflake.
Skills: Kafka, Kinesis, Spark, streaming, event processing, recovery.


Pipeline C — Governance and operational monitoring
Add schema validation, personally identifiable information (PII) masking, role-based access, pipeline run logs, freshness checks, failure alerts, retries and a CI/CD deployment workflow.
Skills: Security, governance, observability, Ansible, Jenkins, Jira, incident management.
3. Implement the core Snowflake pipeline
Start with a retail analytics use case. It is easy to explain to interviewers and lets you demonstrate data engineering skills without needing a real company's data.
Example: customer orders pipeline
BUSINESS SCENARIO
A retailer needs reliable daily sales reports and near-real-time order monitoring.
  • Source data: customer records, orders and order events.
  • Business output: revenue by day, order status and customer segments.
  • Privacy requirement: customer emails and phone numbers must not be exposed to every analyst.
  • Operational requirement: detect missing records, duplicates, stale data and failed loads.
Step 1 — Create the data layers
CREATE DATABASE RETAIL_DWH;
CREATE SCHEMA RETAIL_DWH.BRONZE;
CREATE SCHEMA RETAIL_DWH.SILVER;
CREATE SCHEMA RETAIL_DWH.GOLD;
CREATE TABLE RETAIL_DWH.BRONZE.RAW_ORDERS (
    ORDER_ID       NUMBER,
    CUSTOMER_ID    NUMBER,
    ORDER_TS       TIMESTAMP_NTZ,
    STATUS         VARCHAR,
    AMOUNT         NUMBER(12,2),
    INGESTED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
Load sample CSV files initially using COPY INTO. Later, automate ingestion with an S3 stage and Snowpipe.
Step 2 — Clean and validate the data
CREATE OR REPLACE VIEW RETAIL_DWH.SILVER.CLEAN_ORDERS AS
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_TS,
    LOWER(TRIM(STATUS)) AS STATUS,
    AMOUNT,
    INGESTED_AT
FROM RETAIL_DWH.BRONZE.RAW_ORDERS
WHERE ORDER_ID IS NOT NULL
  AND CUSTOMER_ID IS NOT NULL
  AND AMOUNT >= 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ORDER_ID
    ORDER BY INGESTED_AT DESC
) = 1;
This removes invalid records and retains the latest ingested version of each order. In a production system, send rejected records to a quarantine table rather than silently discarding them, and use a deterministic source version or event timestamp when selecting the latest record.
Step 3 — Build an automated Gold layer
CREATE OR REPLACE DYNAMIC TABLE
    RETAIL_DWH.GOLD.DAILY_SALES
    TARGET_LAG = '10 minutes'
    WAREHOUSE = TRANSFORM_WH
AS
SELECT
    TO_DATE(ORDER_TS) AS SALES_DATE,
    STATUS,
    COUNT(*) AS ORDER_COUNT,
    SUM(AMOUNT) AS TOTAL_REVENUE,
    AVG(AMOUNT) AS AVG_ORDER_VALUE
FROM RETAIL_DWH.SILVER.CLEAN_ORDERS
GROUP BY 1, 2;
Snowflake maintains this business-level aggregation as its source changes, subject to its refresh configuration and supported incremental processing. Dynamic Tables are well suited to declarative SQL transformations. 
Snowflake Documentation
+1
Step 4 — Add change data capture with Streams and Tasks
For a second implementation, use Streams and Tasks to demonstrate procedural control.
CREATE OR REPLACE STREAM
    RETAIL_DWH.BRONZE.ORDER_CHANGES
    ON TABLE RETAIL_DWH.BRONZE.RAW_ORDERS;
CREATE TABLE IF NOT EXISTS
    RETAIL_DWH.SILVER.ORDER_AUDIT (
        ORDER_ID NUMBER,
        STATUS VARCHAR,
        AMOUNT NUMBER(12,2),
        PROCESSED_AT TIMESTAMP_NTZ
    );
CREATE OR REPLACE TASK
    RETAIL_DWH.BRONZE.PROCESS_ORDER_CHANGES
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA(
        'RETAIL_DWH.BRONZE.ORDER_CHANGES'
    )
AS
INSERT INTO RETAIL_DWH.SILVER.ORDER_AUDIT
    (ORDER_ID, STATUS, AMOUNT, PROCESSED_AT)
SELECT
    ORDER_ID, STATUS, AMOUNT, CURRENT_TIMESTAMP()
FROM RETAIL_DWH.BRONZE.ORDER_CHANGES
WHERE METADATA$ACTION = 'INSERT';
ALTER TASK RETAIL_DWH.BRONZE.PROCESS_ORDER_CHANGES RESUME;
This example captures inserted records for an audit-style table. For a real CDC pipeline, you would also handle updates and deletes, establish idempotency, and design the audit table to preserve the required history. Streams expose change records, while Tasks execute the processing logic. 
Snowflake Documentation
+1
Important: Dynamic Tables and Streams/Tasks are alternative approaches for many transformations, but they can also coexist. Implement both as separate demonstrations or combine them where procedural operations are required. 

Snowflake Documentation
+1
4. Add data privacy, security and governance
These controls are essential for this particular role.
Requirement	Implementation
Least-privilege access	Separate ingestion, transformation and analyst roles
PII protection	Mask email and phone fields using masking policies or secure views
Encryption	Use secure transport and managed encryption for data at rest
Data quality	Check uniqueness, nulls, valid statuses, amounts and referential integrity
Governance	Document table owners, business definitions, sensitivity and retention
Auditability	Record pipeline runs, source file names, row counts and processing timestamps
Data deletion	Implement an approved deletion workflow across downstream datasets and copies
For example, a basic role-dependent email masking policy could look like this:
CREATE OR REPLACE MASKING POLICY
    RETAIL_DWH.SILVER.EMAIL_MASK AS (VAL VARCHAR)
    RETURNS VARCHAR ->
        CASE
            WHEN CURRENT_ROLE() = 'PII_ACCESS_ROLE'
                THEN VAL
            ELSE '***MASKED***'
        END;
Apply it to a customer email column:
ALTER TABLE RETAIL_DWH.SILVER.CUSTOMERS
MODIFY COLUMN EMAIL
SET MASKING POLICY RETAIL_DWH.SILVER.EMAIL_MASK;
This is an illustrative policy, not a complete security design. Restrict who can assume PII_ACCESS_ROLE, test the policy using actual role grants, and account for Snowflake edition requirements for dynamic data masking. 

Snowflake Documentation
5. Implement the AWS and streaming components


AWS S3 + Lambda + Snowpipe
Automated batch ingestion
  1. Upload daily order files into an S3 incoming/ prefix.
  2. Configure an S3 event notification to invoke Lambda or a supported event-processing workflow.
  3. Configure Snowflake storage integration and Snowpipe auto-ingest.
  4. Track file processing, errors and duplicate deliveries.
S3 notifications are delivered at least once, so make ingestion idempotent and avoid notification loops. 

AWS Lambda
Kafka or Kinesis + Spark
Streaming events
  1. Generate purchase events with an event ID, order ID and event timestamp.
  2. Publish them to Kafka or Kinesis.
  3. Process events using Spark Structured Streaming, with checkpointing and duplicate detection.
  4. Write durable results to Delta Lake on S3 or ingest them into Snowflake.
Start with Kafka locally if you want to limit cloud costs. Add Kinesis as a separate AWS integration once the streaming logic works.
6. Add operational excellence and CI/CD
Your portfolio should show how you maintain pipelines, not only how you build them.
Area	What to demonstrate
Observability	Run status, duration, records processed, rejected records and freshness
Reliability	Retry transient failures, checkpoint streaming jobs and support safe reruns
Alerting	Alert on failed loads, delayed data and abnormal record counts
SQL optimization	Inspect query profiles, reduce unnecessary scans and test incremental processing
CI/CD	GitHub pull requests, SQL/Python tests and automated deployment using Jenkins
Infrastructure as code	Ansible or CloudFormation for repeatable environment setup
Collaboration	Jira stories, Confluence runbooks, incident reports and stakeholder requirements

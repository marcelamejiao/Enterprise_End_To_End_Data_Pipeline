-- Freshness and volume checks against the Gold layer and the pipeline run
-- log (snowflake/05_streams_tasks.sql). Run on a schedule (Task or external
-- scheduler) and feed results to monitoring/alerting.py.

-- 1. Staleness: Gold's newest sales date should track within a day of "now".
CREATE OR REPLACE VIEW RETAIL_DWH.GOLD.FRESHNESS_CHECK AS
SELECT
    MAX(SALES_DATE) AS LATEST_SALES_DATE,
    DATEDIFF('day', MAX(SALES_DATE), CURRENT_DATE()) AS DAYS_STALE,
    DATEDIFF('day', MAX(SALES_DATE), CURRENT_DATE()) > 1 AS IS_STALE
FROM RETAIL_DWH.GOLD.DAILY_SALES;

-- 2. Abnormal volume: flag a load whose row count deviates sharply from the
-- trailing 7-day average (catches both failed loads and duplicate storms).
CREATE OR REPLACE VIEW RETAIL_DWH.SILVER.VOLUME_ANOMALY_CHECK AS
WITH daily_counts AS (
    SELECT
        TO_DATE(INGESTED_AT) AS LOAD_DATE,
        COUNT(*) AS ROW_COUNT
    FROM RETAIL_DWH.BRONZE.RAW_ORDERS
    GROUP BY 1
),
with_baseline AS (
    SELECT
        LOAD_DATE,
        ROW_COUNT,
        AVG(ROW_COUNT) OVER (
            ORDER BY LOAD_DATE
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS TRAILING_AVG
    FROM daily_counts
)
SELECT
    LOAD_DATE,
    ROW_COUNT,
    TRAILING_AVG,
    CASE
        WHEN TRAILING_AVG IS NULL THEN FALSE
        WHEN ROW_COUNT < 0.5 * TRAILING_AVG THEN TRUE   -- missing records
        WHEN ROW_COUNT > 2.0 * TRAILING_AVG THEN TRUE   -- duplicate storm
        ELSE FALSE
    END AS IS_ANOMALY
FROM with_baseline;

-- 3. Failed/long-running pipeline runs from the run log.
CREATE OR REPLACE VIEW RETAIL_DWH.SILVER.FAILED_RUNS AS
SELECT *
FROM RETAIL_DWH.SILVER.PIPELINE_RUN_LOG
WHERE STATUS != 'success'
   OR DATEDIFF('minute', RUN_STARTED_AT, RUN_ENDED_AT) > 30;

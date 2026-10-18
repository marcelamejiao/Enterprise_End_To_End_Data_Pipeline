-- Bronze layer ingestion: file format, external stage, and manual load.
-- Manual COPY INTO first (Phase 1 demo); automate via storage integration + Snowpipe
-- once this path is proven (see aws/s3_setup.md).

CREATE FILE FORMAT IF NOT EXISTS RETAIL_DWH.BRONZE.CSV_STANDARD
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Storage integration and stage are created per-environment with real AWS
-- credentials; this is a placeholder showing the shape used by Snowpipe.
-- CREATE STORAGE INTEGRATION IF NOT EXISTS RETAIL_S3_INT ...
-- CREATE STAGE IF NOT EXISTS RETAIL_DWH.BRONZE.ORDERS_STAGE
--     URL = 's3://<bucket>/incoming/'
--     STORAGE_INTEGRATION = RETAIL_S3_INT
--     FILE_FORMAT = RETAIL_DWH.BRONZE.CSV_STANDARD;

-- Manual load for local development / demo, loading from a local stage
-- populated via `snowsql -q "PUT file://data/sample_orders.csv @%RAW_ORDERS"`.
COPY INTO RETAIL_DWH.BRONZE.RAW_ORDERS
    (ORDER_ID, CUSTOMER_ID, ORDER_TS, STATUS, AMOUNT, SOURCE_FILE)
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        METADATA$FILENAME
    FROM @%RAW_ORDERS
)
FILE_FORMAT = RETAIL_DWH.BRONZE.CSV_STANDARD
ON_ERROR = 'CONTINUE';

COPY INTO RETAIL_DWH.BRONZE.RAW_CUSTOMERS
    (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, SIGNUP_TS)
FROM @%RAW_CUSTOMERS
FILE_FORMAT = RETAIL_DWH.BRONZE.CSV_STANDARD
ON_ERROR = 'CONTINUE';

-- Snowpipe definition (auto-ingest once the S3 event notification is wired up).
CREATE PIPE IF NOT EXISTS RETAIL_DWH.BRONZE.ORDERS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO RETAIL_DWH.BRONZE.RAW_ORDERS
    (ORDER_ID, CUSTOMER_ID, ORDER_TS, STATUS, AMOUNT, SOURCE_FILE)
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        METADATA$FILENAME
    FROM @RETAIL_DWH.BRONZE.ORDERS_STAGE
)
FILE_FORMAT = RETAIL_DWH.BRONZE.CSV_STANDARD
ON_ERROR = 'CONTINUE';

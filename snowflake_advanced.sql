-- ============================================================
-- Insurance Claim Risk Analytics
-- Module 6: Snowflake – Advanced Features
-- Covers: Clustering, Time Travel, Semi-Structured Data,
--         RBAC, Data Masking, Snowpipe, Materialized Views
-- ============================================================

-- ============================================================
-- 1. DATABASE & WAREHOUSE SETUP
-- ============================================================
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS INSURANCE_DWH;
USE DATABASE INSURANCE_DWH;

CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS DWH;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Virtual Warehouses: separate compute for ETL vs BI
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
    WAREHOUSE_SIZE   = 'SMALL'
    AUTO_SUSPEND     = 120
    AUTO_RESUME      = TRUE
    COMMENT          = 'ETL/ELT processing warehouse';

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT          = 'BI dashboard queries';

USE WAREHOUSE ETL_WH;


-- ============================================================
-- 2. DIMENSION TABLES
-- ============================================================
USE SCHEMA DWH;

CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY        INTEGER         PRIMARY KEY,
    FULL_DATE       DATE            NOT NULL,
    YEAR            SMALLINT,
    QUARTER         SMALLINT,
    MONTH           SMALLINT,
    MONTH_NAME      VARCHAR(15),
    WEEK            SMALLINT,
    DAY_OF_WEEK     SMALLINT,
    DAY_NAME        VARCHAR(15),
    IS_WEEKEND      BOOLEAN,
    IS_HOLIDAY      BOOLEAN
);

CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    CUSTOMER_KEY    NUMBER          AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID     VARCHAR(20)     NOT NULL,
    FIRST_NAME      VARCHAR(100),
    LAST_NAME       VARCHAR(100),
    AGE             NUMBER(3),
    AGE_BAND        VARCHAR(20),
    GENDER          VARCHAR(20),
    MARITAL_STATUS  VARCHAR(30),
    OCCUPATION      VARCHAR(50),
    ANNUAL_INCOME   NUMBER(15,2),
    INCOME_BAND     VARCHAR(20),
    CREDIT_SCORE    NUMBER(4),
    CREDIT_BAND     VARCHAR(20),
    CITY            VARCHAR(100),
    STATE           VARCHAR(100),
    IS_CURRENT      BOOLEAN         DEFAULT TRUE,
    EFF_START_DATE  DATE            DEFAULT CURRENT_DATE(),
    EFF_END_DATE    DATE            DEFAULT '9999-12-31'
);

CREATE OR REPLACE TABLE DIM_POLICY_TYPE (
    POLICY_TYPE_KEY     NUMBER      AUTOINCREMENT PRIMARY KEY,
    POLICY_TYPE_ID      VARCHAR(10) NOT NULL,
    POLICY_TYPE_NAME    VARCHAR(100),
    CATEGORY            VARCHAR(50),
    BASE_PREMIUM_RATE   NUMBER(6,4),
    MAX_COVERAGE_AMOUNT NUMBER(15,2),
    RISK_LEVEL          VARCHAR(20)
);

CREATE OR REPLACE TABLE DIM_AGENTS (
    AGENT_KEY           NUMBER      AUTOINCREMENT PRIMARY KEY,
    AGENT_ID            VARCHAR(10) NOT NULL,
    AGENT_NAME          VARCHAR(200),
    REGION              VARCHAR(50),
    EXPERIENCE_YEARS    NUMBER(3),
    EXPERIENCE_BAND     VARCHAR(20),
    PERFORMANCE_RATING  NUMBER(3,1),
    PERFORMANCE_BAND    VARCHAR(20)
);


-- ============================================================
-- 3. FACT TABLE WITH CLUSTERING KEY
-- ============================================================
CREATE OR REPLACE TABLE FACT_CLAIMS (
    CLAIM_KEY           NUMBER          AUTOINCREMENT PRIMARY KEY,
    CLAIM_ID            VARCHAR(20)     NOT NULL,
    POLICY_ID           VARCHAR(20),
    CUSTOMER_KEY        NUMBER          REFERENCES DIM_CUSTOMERS(CUSTOMER_KEY),
    POLICY_TYPE_KEY     NUMBER          REFERENCES DIM_POLICY_TYPE(POLICY_TYPE_KEY),
    AGENT_KEY           NUMBER          REFERENCES DIM_AGENTS(AGENT_KEY),
    CLAIM_DATE_KEY      NUMBER          REFERENCES DIM_DATE(DATE_KEY),
    CLAIM_TYPE          VARCHAR(50),
    CLAIMED_AMOUNT      NUMBER(15,2),
    APPROVED_AMOUNT     NUMBER(15,2),
    REJECTED_AMOUNT     NUMBER(15,2)    AS (CLAIMED_AMOUNT - APPROVED_AMOUNT),
    PAYOUT_RATIO        NUMBER(8,4),
    PROCESSING_DAYS     NUMBER(4),
    FRAUD_FLAG          BOOLEAN,
    FRAUD_SCORE         NUMBER(6,4),
    RISK_SCORE          NUMBER(5,2),
    CLAIM_STATUS        VARCHAR(50),
    LOAD_DATE           DATE            DEFAULT CURRENT_DATE()
)
-- Clustering on high-cardinality query filters
CLUSTER BY (CLAIM_DATE_KEY, CLAIM_TYPE);

-- Check clustering depth (run after data load)
-- SELECT SYSTEM$CLUSTERING_INFORMATION('FACT_CLAIMS');

-- Automatic clustering (Snowflake manages re-clustering)
ALTER TABLE FACT_CLAIMS CLUSTER BY (CLAIM_DATE_KEY, CLAIM_TYPE);


-- ============================================================
-- 4. SEMI-STRUCTURED DATA (VARIANT / JSON)
-- ============================================================
CREATE OR REPLACE TABLE CLAIMS_SEMI_STRUCTURED (
    CLAIM_ID        VARCHAR(20),
    RAW_JSON        VARIANT,                    -- Semi-structured column
    LOAD_TIMESTAMP  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load JSON from stage (simulated)
-- COPY INTO CLAIMS_SEMI_STRUCTURED
-- FROM @MY_STAGE/claims_semi_structured.json
-- FILE_FORMAT = (TYPE = 'JSON');

-- Query VARIANT columns using dot notation & FLATTEN
SELECT
    CLAIM_ID,
    RAW_JSON:claim_id::STRING                               AS claim_id_json,
    RAW_JSON:metadata:claim_type::STRING                    AS claim_type,
    RAW_JSON:metadata:status::STRING                        AS status,
    RAW_JSON:metadata:fraud_indicators:is_fraud::BOOLEAN    AS is_fraud,
    RAW_JSON:metadata:fraud_indicators:fraud_score::FLOAT   AS fraud_score,
    RAW_JSON:financial:claimed_amount::FLOAT                AS claimed_amount,
    RAW_JSON:financial:approved_amount::FLOAT               AS approved_amount,
    -- FLATTEN nested array of fraud flags
    f.VALUE::STRING                                         AS fraud_indicator_flag
FROM CLAIMS_SEMI_STRUCTURED,
LATERAL FLATTEN(INPUT => RAW_JSON:metadata:fraud_indicators:flags) f
WHERE RAW_JSON:metadata:fraud_indicators:is_fraud::BOOLEAN = TRUE
ORDER BY fraud_score DESC;

-- Parse Avro/Parquet semi-structured (concept)
-- SELECT $1:field_name FROM @STAGE/file.parquet (FILE_FORMAT=PARQUET);


-- ============================================================
-- 5. TIME TRAVEL
-- ============================================================
-- Query data as it was 1 hour ago
SELECT COUNT(*) FROM FACT_CLAIMS
AT (OFFSET => -3600);           -- 3600 seconds = 1 hour ago

-- Query at specific timestamp
SELECT * FROM FACT_CLAIMS
AT (TIMESTAMP => '2025-01-15 09:00:00'::TIMESTAMP_NTZ)
WHERE FRAUD_FLAG = TRUE;

-- Restore accidentally deleted rows
-- CREATE OR REPLACE TABLE FACT_CLAIMS AS
--     SELECT * FROM FACT_CLAIMS BEFORE (STATEMENT => '<query_id>');

-- Zero-copy clone (dev/test environment from prod snapshot)
CREATE OR REPLACE TABLE FACT_CLAIMS_DEV
    CLONE FACT_CLAIMS;          -- Instant, no extra storage until changes

-- Verify clone
SELECT COUNT(*) FROM FACT_CLAIMS_DEV;


-- ============================================================
-- 6. ADVANCED CTEs & WINDOW FUNCTIONS (Snowflake dialect)
-- ============================================================
WITH MONTHLY_FRAUD AS (
    SELECT
        D.YEAR,
        D.MONTH,
        D.MONTH_NAME,
        FC.CLAIM_TYPE,
        COUNT(*)                                    AS TOTAL_CLAIMS,
        SUM(IFF(FC.FRAUD_FLAG, 1, 0))               AS FRAUD_CLAIMS,
        ROUND(AVG(FC.FRAUD_SCORE), 4)               AS AVG_FRAUD_SCORE,
        SUM(FC.CLAIMED_AMOUNT)                      AS TOTAL_CLAIMED
    FROM FACT_CLAIMS FC
    JOIN DIM_DATE D ON FC.CLAIM_DATE_KEY = D.DATE_KEY
    GROUP BY D.YEAR, D.MONTH, D.MONTH_NAME, FC.CLAIM_TYPE
),
RANKED AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY YEAR, MONTH
            ORDER BY FRAUD_CLAIMS DESC
        )                                           AS FRAUD_RANK,
        SUM(FRAUD_CLAIMS) OVER (
            PARTITION BY CLAIM_TYPE, YEAR
            ORDER BY MONTH
        )                                           AS YTD_FRAUD_CLAIMS,
        LAG(FRAUD_CLAIMS) OVER (
            PARTITION BY CLAIM_TYPE ORDER BY YEAR, MONTH
        )                                           AS PREV_MONTH_FRAUD
    FROM MONTHLY_FRAUD
)
SELECT * FROM RANKED
WHERE FRAUD_RANK <= 3
ORDER BY YEAR, MONTH, FRAUD_RANK;


-- ============================================================
-- 7. DATA LOADING: COPY INTO + SNOWPIPE
-- ============================================================
-- Create file format
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '');

-- Create named stage
CREATE OR REPLACE STAGE INSURANCE_STAGE
    FILE_FORMAT = CSV_FORMAT
    COMMENT = 'Stage for insurance data files';

-- Bulk load
-- COPY INTO FACT_CLAIMS
-- FROM @INSURANCE_STAGE/fact_claims.csv
-- FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
-- ON_ERROR = 'CONTINUE';

-- Snowpipe: auto-ingest (event-driven)
CREATE OR REPLACE PIPE CLAIMS_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO STAGING.STG_CLAIMS
    FROM @INSURANCE_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT');

-- Monitor pipe
SELECT SYSTEM$PIPE_STATUS('CLAIMS_PIPE');

-- Unload data
COPY INTO @INSURANCE_STAGE/export/claims_export_
FROM (
    SELECT * FROM FACT_CLAIMS WHERE FRAUD_FLAG = TRUE
)
FILE_FORMAT = (TYPE = 'PARQUET')
OVERWRITE = TRUE;


-- ============================================================
-- 8. SECURITY: RBAC + DATA MASKING + ROW-LEVEL SECURITY
-- ============================================================
USE ROLE SECURITYADMIN;

-- Roles
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS DATA_STEWARD_ROLE;
CREATE ROLE IF NOT EXISTS CLAIMS_MANAGER_ROLE;

-- Grant hierarchy
GRANT ROLE ANALYST_ROLE      TO ROLE DATA_STEWARD_ROLE;
GRANT ROLE DATA_STEWARD_ROLE TO ROLE CLAIMS_MANAGER_ROLE;
GRANT ROLE CLAIMS_MANAGER_ROLE TO ROLE SYSADMIN;

-- Warehouse access
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE ETL_WH        TO ROLE CLAIMS_MANAGER_ROLE;

-- Schema/Table grants
USE ROLE SYSADMIN;
GRANT USAGE  ON DATABASE  INSURANCE_DWH           TO ROLE ANALYST_ROLE;
GRANT USAGE  ON SCHEMA    INSURANCE_DWH.DWH        TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA INSURANCE_DWH.DWH TO ROLE ANALYST_ROLE;

-- Data Masking Policy: mask annual income for non-privileged roles
CREATE OR REPLACE MASKING POLICY INCOME_MASK AS (INCOME NUMBER)
RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('CLAIMS_MANAGER_ROLE', 'SYSADMIN') THEN INCOME
        ELSE -1     -- mask with sentinel value
    END;

ALTER TABLE DIM_CUSTOMERS
    MODIFY COLUMN ANNUAL_INCOME
    SET MASKING POLICY INCOME_MASK;

-- Dynamic Data Masking for credit score
CREATE OR REPLACE MASKING POLICY CREDIT_SCORE_MASK AS (SCORE NUMBER)
RETURNS NUMBER ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_STEWARD_ROLE', 'SYSADMIN') THEN SCORE
        ELSE 0
    END;

ALTER TABLE DIM_CUSTOMERS
    MODIFY COLUMN CREDIT_SCORE
    SET MASKING POLICY CREDIT_SCORE_MASK;

-- Row-Level Security: analysts see only their region's claims
CREATE OR REPLACE ROW ACCESS POLICY REGION_POLICY AS (REGION VARCHAR)
RETURNS BOOLEAN ->
    CURRENT_ROLE() IN ('SYSADMIN', 'CLAIMS_MANAGER_ROLE')
    OR REGION = CURRENT_USER()   -- user name matches region (demo)
    OR IS_ROLE_IN_SESSION('DATA_STEWARD_ROLE');

ALTER TABLE DIM_AGENTS ADD ROW ACCESS POLICY REGION_POLICY ON (REGION);


-- ============================================================
-- 9. MATERIALIZED VIEWS + PERFORMANCE TUNING
-- ============================================================
CREATE OR REPLACE MATERIALIZED VIEW ANALYTICS.MV_FRAUD_SUMMARY AS
SELECT
    D.YEAR,
    D.QUARTER,
    PT.POLICY_TYPE_NAME,
    A.REGION,
    COUNT(FC.CLAIM_KEY)                         AS CLAIM_COUNT,
    SUM(IFF(FC.FRAUD_FLAG, 1, 0))               AS FRAUD_COUNT,
    ROUND(AVG(FC.FRAUD_SCORE), 4)               AS AVG_FRAUD_SCORE,
    ROUND(SUM(FC.CLAIMED_AMOUNT), 2)            AS TOTAL_CLAIMED,
    ROUND(SUM(FC.APPROVED_AMOUNT), 2)           AS TOTAL_APPROVED
FROM FACT_CLAIMS FC
JOIN DIM_DATE        D  ON FC.CLAIM_DATE_KEY  = D.DATE_KEY
JOIN DIM_POLICY_TYPE PT ON FC.POLICY_TYPE_KEY = PT.POLICY_TYPE_KEY
JOIN DIM_AGENTS      A  ON FC.AGENT_KEY       = A.AGENT_KEY
GROUP BY D.YEAR, D.QUARTER, PT.POLICY_TYPE_NAME, A.REGION;

-- Query the MV (Snowflake serves from pre-computed result)
SELECT * FROM ANALYTICS.MV_FRAUD_SUMMARY
WHERE YEAR = 2024 AND FRAUD_COUNT > 10
ORDER BY AVG_FRAUD_SCORE DESC;

-- Data Sharing (Secure Share)
-- CREATE SHARE INSURANCE_SHARE;
-- GRANT USAGE ON DATABASE INSURANCE_DWH TO SHARE INSURANCE_SHARE;
-- GRANT SELECT ON ANALYTICS.MV_FRAUD_SUMMARY TO SHARE INSURANCE_SHARE;
-- ALTER SHARE INSURANCE_SHARE ADD ACCOUNTS = <partner_account>;

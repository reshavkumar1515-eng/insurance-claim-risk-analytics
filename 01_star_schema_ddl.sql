-- ============================================================
-- Insurance Claim Risk Analytics
-- Module 1: Data Warehouse Star Schema Design
-- Author: Wipro DAI-DATA Batch
-- ============================================================

-- ============================================================
-- STAGING LAYER
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id     VARCHAR(20),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    age             INTEGER,
    gender          VARCHAR(20),
    marital_status  VARCHAR(30),
    occupation      VARCHAR(50),
    annual_income   DECIMAL(15,2),
    credit_score    INTEGER,
    city            VARCHAR(100),
    state           VARCHAR(100),
    created_date    DATE,
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system   VARCHAR(50) DEFAULT 'CRM_SYSTEM'
);

CREATE TABLE IF NOT EXISTS staging.stg_policies (
    policy_id           VARCHAR(20),
    customer_id         VARCHAR(20),
    policy_type_id      VARCHAR(10),
    agent_id            VARCHAR(10),
    start_date          DATE,
    end_date            DATE,
    premium_amount      DECIMAL(15,2),
    coverage_amount     DECIMAL(15,2),
    deductible_amount   DECIMAL(15,2),
    status              VARCHAR(30),
    risk_score          DECIMAL(5,2),
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_claims (
    claim_id            VARCHAR(20),
    policy_id           VARCHAR(20),
    claim_date          DATE,
    claim_type          VARCHAR(50),
    claimed_amount      DECIMAL(15,2),
    approved_amount     DECIMAL(15,2),
    status              VARCHAR(50),
    fraud_flag          SMALLINT,
    fraud_score         DECIMAL(6,4),
    processing_days     INTEGER,
    adjuster_notes      TEXT,
    load_timestamp      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- PRESENTATION LAYER – DIMENSION TABLES (Star Schema)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dwh;

-- DIM: Date (Conformed Dimension)
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INTEGER PRIMARY KEY,   -- surrogate key: YYYYMMDD
    full_date       DATE NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(15),
    week            SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(15),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN,
    fiscal_year     SMALLINT,
    fiscal_quarter  SMALLINT
);

-- DIM: Customers (SCD Type 2 ready)
CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_key    SERIAL PRIMARY KEY,           -- surrogate key
    customer_id     VARCHAR(20) NOT NULL,         -- natural key
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    full_name       VARCHAR(200) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    age             INTEGER,
    age_band        VARCHAR(20),                  -- '18-25', '26-35', etc.
    gender          VARCHAR(20),
    marital_status  VARCHAR(30),
    occupation      VARCHAR(50),
    annual_income   DECIMAL(15,2),
    income_band     VARCHAR(20),                  -- 'Low', 'Medium', 'High'
    credit_score    INTEGER,
    credit_band     VARCHAR(20),                  -- 'Poor', 'Fair', 'Good', 'Excellent'
    city            VARCHAR(100),
    state           VARCHAR(100),
    -- SCD Type 2 columns
    eff_start_date  DATE DEFAULT CURRENT_DATE,
    eff_end_date    DATE DEFAULT '9999-12-31',
    is_current      BOOLEAN DEFAULT TRUE,
    created_date    DATE,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM: Policy Types
CREATE TABLE IF NOT EXISTS dwh.dim_policy_type (
    policy_type_key     SERIAL PRIMARY KEY,
    policy_type_id      VARCHAR(10) NOT NULL,
    policy_type_name    VARCHAR(100),
    category            VARCHAR(50),
    base_premium_rate   DECIMAL(6,4),
    max_coverage_amount DECIMAL(15,2),
    risk_level          VARCHAR(20)     -- derived: 'Low', 'Medium', 'High'
);

-- DIM: Agents
CREATE TABLE IF NOT EXISTS dwh.dim_agents (
    agent_key           SERIAL PRIMARY KEY,
    agent_id            VARCHAR(10) NOT NULL,
    agent_name          VARCHAR(200),
    region              VARCHAR(50),
    experience_years    INTEGER,
    experience_band     VARCHAR(20),   -- 'Junior', 'Mid', 'Senior', 'Expert'
    performance_rating  DECIMAL(3,1),
    performance_band    VARCHAR(20)    -- 'Low', 'Average', 'High'
);

-- DIM: Claim Types (small lookup)
CREATE TABLE IF NOT EXISTS dwh.dim_claim_type (
    claim_type_key  SERIAL PRIMARY KEY,
    claim_type_name VARCHAR(50),
    risk_category   VARCHAR(30),     -- 'High', 'Medium', 'Low'
    avg_payout_pct  DECIMAL(5,2)     -- historical average payout percentage
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- FACT: Policies (Accumulating Snapshot)
CREATE TABLE IF NOT EXISTS dwh.fact_policies (
    policy_key          BIGSERIAL PRIMARY KEY,
    policy_id           VARCHAR(20) NOT NULL,
    customer_key        INTEGER REFERENCES dwh.dim_customers(customer_key),
    policy_type_key     INTEGER REFERENCES dwh.dim_policy_type(policy_type_key),
    agent_key           INTEGER REFERENCES dwh.dim_agents(agent_key),
    start_date_key      INTEGER REFERENCES dwh.dim_date(date_key),
    end_date_key        INTEGER REFERENCES dwh.dim_date(date_key),
    -- Measures
    premium_amount      DECIMAL(15,2),
    coverage_amount     DECIMAL(15,2),
    deductible_amount   DECIMAL(15,2),
    risk_score          DECIMAL(5,2),
    policy_duration_days INTEGER,
    -- Status
    policy_status       VARCHAR(30),
    -- Audit
    load_date           DATE DEFAULT CURRENT_DATE
)
PARTITION BY RANGE (start_date_key);

-- Partitions by year
CREATE TABLE IF NOT EXISTS dwh.fact_policies_2020
    PARTITION OF dwh.fact_policies
    FOR VALUES FROM (20200101) TO (20210101);

CREATE TABLE IF NOT EXISTS dwh.fact_policies_2021
    PARTITION OF dwh.fact_policies
    FOR VALUES FROM (20210101) TO (20220101);

CREATE TABLE IF NOT EXISTS dwh.fact_policies_2022
    PARTITION OF dwh.fact_policies
    FOR VALUES FROM (20220101) TO (20230101);

CREATE TABLE IF NOT EXISTS dwh.fact_policies_2023
    PARTITION OF dwh.fact_policies
    FOR VALUES FROM (20230101) TO (20240101);

CREATE TABLE IF NOT EXISTS dwh.fact_policies_2024
    PARTITION OF dwh.fact_policies
    FOR VALUES FROM (20240101) TO (20250101);

-- FACT: Claims (Transaction Fact – main analytics table)
CREATE TABLE IF NOT EXISTS dwh.fact_claims (
    claim_key           BIGSERIAL PRIMARY KEY,
    claim_id            VARCHAR(20) NOT NULL,
    policy_key          BIGINT REFERENCES dwh.fact_policies(policy_key),
    customer_key        INTEGER REFERENCES dwh.dim_customers(customer_key),
    policy_type_key     INTEGER REFERENCES dwh.dim_policy_type(policy_type_key),
    agent_key           INTEGER REFERENCES dwh.dim_agents(agent_key),
    claim_type_key      INTEGER REFERENCES dwh.dim_claim_type(claim_type_key),
    claim_date_key      INTEGER REFERENCES dwh.dim_date(date_key),
    -- Measures
    claimed_amount      DECIMAL(15,2),
    approved_amount     DECIMAL(15,2),
    rejected_amount     DECIMAL(15,2) GENERATED ALWAYS AS (claimed_amount - approved_amount) STORED,
    payout_ratio        DECIMAL(8,4),
    processing_days     INTEGER,
    -- Risk / Fraud
    fraud_flag          BOOLEAN,
    fraud_score         DECIMAL(6,4),
    risk_score          DECIMAL(5,2),
    -- Status
    claim_status        VARCHAR(50),
    -- Audit
    load_date           DATE DEFAULT CURRENT_DATE
)
PARTITION BY RANGE (claim_date_key);

CREATE TABLE IF NOT EXISTS dwh.fact_claims_2020
    PARTITION OF dwh.fact_claims FOR VALUES FROM (20200101) TO (20210101);
CREATE TABLE IF NOT EXISTS dwh.fact_claims_2021
    PARTITION OF dwh.fact_claims FOR VALUES FROM (20210101) TO (20220101);
CREATE TABLE IF NOT EXISTS dwh.fact_claims_2022
    PARTITION OF dwh.fact_claims FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE IF NOT EXISTS dwh.fact_claims_2023
    PARTITION OF dwh.fact_claims FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE IF NOT EXISTS dwh.fact_claims_2024
    PARTITION OF dwh.fact_claims FOR VALUES FROM (20240101) TO (20250101);

-- ============================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_claims_policy_key
    ON dwh.fact_claims (policy_key);

CREATE INDEX IF NOT EXISTS idx_fact_claims_customer_key
    ON dwh.fact_claims (customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_claims_fraud
    ON dwh.fact_claims (fraud_flag) WHERE fraud_flag = TRUE;

CREATE INDEX IF NOT EXISTS idx_fact_claims_date_status
    ON dwh.fact_claims (claim_date_key, claim_status);

CREATE INDEX IF NOT EXISTS idx_dim_customers_natural
    ON dwh.dim_customers (customer_id, is_current);

CREATE INDEX IF NOT EXISTS idx_fact_policies_customer
    ON dwh.fact_policies (customer_key);

-- ============================================================
-- DATA DICTIONARY / METADATA
-- ============================================================

CREATE TABLE IF NOT EXISTS dwh.data_dictionary (
    table_schema    VARCHAR(50),
    table_name      VARCHAR(100),
    column_name     VARCHAR(100),
    data_type       VARCHAR(50),
    description     TEXT,
    pii_flag        BOOLEAN DEFAULT FALSE,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO dwh.data_dictionary VALUES
('dwh','fact_claims','fraud_score',   'DECIMAL','ML model score (0=clean, 1=fraud)',FALSE,NOW()),
('dwh','fact_claims','fraud_flag',    'BOOLEAN','Manual/model fraud label',          FALSE,NOW()),
('dwh','dim_customers','annual_income','DECIMAL','Customer declared annual income',  TRUE, NOW()),
('dwh','dim_customers','credit_score','INTEGER','Credit bureau score 300–850',        TRUE, NOW());

COMMENT ON TABLE dwh.fact_claims    IS 'Central fact table for insurance claim analytics';
COMMENT ON TABLE dwh.dim_customers  IS 'Customer dimension with SCD Type 2 support';
COMMENT ON TABLE dwh.fact_policies  IS 'Policy fact – partitioned by policy start year';

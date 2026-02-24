-- ============================================================
-- Insurance Claim Risk Analytics
-- Module 2: Advanced SQL – CTEs, Window Functions,
--           ROLLUP/CUBE, Indexing, Partitioning
-- ============================================================

-- ============================================================
-- 1. CTE: High-Risk Customer Identification
-- ============================================================
WITH customer_claim_metrics AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.credit_band,
        c.income_band,
        COUNT(fc.claim_key)                         AS total_claims,
        SUM(fc.claimed_amount)                      AS total_claimed,
        SUM(fc.approved_amount)                     AS total_approved,
        AVG(fc.fraud_score)                         AS avg_fraud_score,
        SUM(fc.fraud_flag::INT)                     AS fraud_claim_count
    FROM dwh.fact_claims fc
    JOIN dwh.dim_customers c ON fc.customer_key = c.customer_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_id, c.full_name, c.credit_band, c.income_band
),
risk_classification AS (
    SELECT
        *,
        CASE
            WHEN avg_fraud_score >= 0.7 OR fraud_claim_count >= 3  THEN 'CRITICAL'
            WHEN avg_fraud_score >= 0.4 OR total_claims >= 5        THEN 'HIGH'
            WHEN avg_fraud_score >= 0.2 OR total_claims >= 3        THEN 'MEDIUM'
            ELSE                                                          'LOW'
        END AS risk_level
    FROM customer_claim_metrics
)
SELECT
    customer_id,
    full_name,
    credit_band,
    income_band,
    total_claims,
    ROUND(total_claimed, 2)          AS total_claimed,
    ROUND(avg_fraud_score, 4)        AS avg_fraud_score,
    fraud_claim_count,
    risk_level
FROM risk_classification
WHERE risk_level IN ('CRITICAL', 'HIGH')
ORDER BY avg_fraud_score DESC, total_claims DESC;


-- ============================================================
-- 2. WINDOW FUNCTIONS: Claims Trend & Running Totals
-- ============================================================
WITH monthly_claims AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        pt.policy_type_name,
        COUNT(fc.claim_key)          AS claim_count,
        SUM(fc.claimed_amount)       AS total_claimed,
        SUM(fc.approved_amount)      AS total_approved
    FROM dwh.fact_claims fc
    JOIN dwh.dim_date d     ON fc.claim_date_key = d.date_key
    JOIN dwh.dim_policy_type pt ON fc.policy_type_key = pt.policy_type_key
    GROUP BY d.year, d.month, d.month_name, pt.policy_type_name
)
SELECT
    year,
    month,
    month_name,
    policy_type_name,
    claim_count,
    ROUND(total_claimed, 2)                         AS total_claimed,
    -- Running totals by policy type
    SUM(claim_count)    OVER (
        PARTITION BY policy_type_name, year
        ORDER BY month
        ROWS UNBOUNDED PRECEDING
    )                                               AS ytd_claim_count,
    SUM(total_claimed)  OVER (
        PARTITION BY policy_type_name, year
        ORDER BY month
        ROWS UNBOUNDED PRECEDING
    )                                               AS ytd_claimed_amount,
    -- Month-over-Month growth
    LAG(claim_count, 1) OVER (
        PARTITION BY policy_type_name, year ORDER BY month
    )                                               AS prev_month_count,
    ROUND(
        100.0 * (claim_count - LAG(claim_count,1) OVER (
            PARTITION BY policy_type_name, year ORDER BY month
        )) / NULLIF(LAG(claim_count,1) OVER (
            PARTITION BY policy_type_name, year ORDER BY month
        ), 0), 2
    )                                               AS mom_growth_pct,
    -- Rank by claim amount within year-month
    RANK() OVER (
        PARTITION BY year, month ORDER BY total_claimed DESC
    )                                               AS rank_by_amount
FROM monthly_claims
ORDER BY policy_type_name, year, month;


-- ============================================================
-- 3. WINDOW: Fraud Detection Percentile Scoring
-- ============================================================
SELECT
    fc.claim_id,
    fc.claimed_amount,
    fc.fraud_score,
    fc.claim_status,
    pt.policy_type_name,
    -- Percentile rank of fraud score within policy type
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY fc.policy_type_key
            ORDER BY fc.fraud_score
        ) * 100, 2
    )                                                   AS fraud_percentile,
    -- NTILE into risk buckets
    NTILE(4) OVER (ORDER BY fc.fraud_score DESC)        AS risk_quartile,
    -- Moving average fraud score (7-claim window)
    ROUND(AVG(fc.fraud_score) OVER (
        PARTITION BY fc.policy_type_key
        ORDER BY fc.claim_date_key
        ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
    ), 4)                                               AS rolling_avg_fraud_score
FROM dwh.fact_claims fc
JOIN dwh.dim_policy_type pt ON fc.policy_type_key = pt.policy_type_key
ORDER BY fraud_score DESC;


-- ============================================================
-- 4. ROLLUP: Claims Summary by Region > Policy > Year
-- ============================================================
SELECT
    COALESCE(a.region,          'ALL REGIONS')      AS region,
    COALESCE(pt.policy_type_name,'ALL POLICIES')    AS policy_type,
    COALESCE(d.year::TEXT,       'ALL YEARS')       AS year,
    COUNT(fc.claim_key)                             AS total_claims,
    ROUND(SUM(fc.claimed_amount), 2)                AS total_claimed,
    ROUND(SUM(fc.approved_amount), 2)               AS total_approved,
    ROUND(AVG(fc.payout_ratio)*100, 2)              AS avg_payout_pct,
    SUM(fc.fraud_flag::INT)                         AS fraud_claims
FROM dwh.fact_claims fc
JOIN dwh.dim_agents      a  ON fc.agent_key        = a.agent_key
JOIN dwh.dim_policy_type pt ON fc.policy_type_key  = pt.policy_type_key
JOIN dwh.dim_date        d  ON fc.claim_date_key   = d.date_key
GROUP BY ROLLUP(a.region, pt.policy_type_name, d.year)
ORDER BY region, policy_type, year;


-- ============================================================
-- 5. CUBE: Multi-dimensional Fraud Analysis
-- ============================================================
SELECT
    COALESCE(c.age_band,            'ALL')  AS age_band,
    COALESCE(c.gender,              'ALL')  AS gender,
    COALESCE(pt.category,           'ALL')  AS policy_category,
    COUNT(*)                                AS claim_count,
    SUM(fc.fraud_flag::INT)                 AS fraud_count,
    ROUND(100.0 * SUM(fc.fraud_flag::INT) / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(AVG(fc.fraud_score), 4)           AS avg_fraud_score
FROM dwh.fact_claims fc
JOIN dwh.dim_customers   c  ON fc.customer_key    = c.customer_key
JOIN dwh.dim_policy_type pt ON fc.policy_type_key = pt.policy_type_key
WHERE c.is_current = TRUE
GROUP BY CUBE(c.age_band, c.gender, pt.category)
ORDER BY fraud_rate_pct DESC NULLS LAST;


-- ============================================================
-- 6. CTE + RECURSIVE: Agent Claim Chain (Dependency Trace)
-- ============================================================
WITH RECURSIVE agent_hierarchy AS (
    -- Base: top-level regions
    SELECT
        agent_id,
        agent_name,
        region,
        1 AS level,
        agent_id::TEXT AS path
    FROM dwh.dim_agents
    WHERE experience_band = 'Expert'

    UNION ALL

    SELECT
        a.agent_id,
        a.agent_name,
        a.region,
        ah.level + 1,
        ah.path || ' -> ' || a.agent_id
    FROM dwh.dim_agents a
    JOIN agent_hierarchy ah ON a.region = ah.region AND a.agent_id != ah.agent_id
    WHERE ah.level < 3
)
SELECT DISTINCT agent_id, agent_name, region, level, path
FROM agent_hierarchy
ORDER BY region, level;


-- ============================================================
-- 7. MERGE (Upsert): SCD Type 1 for Policy Status Update
-- ============================================================
-- Simulated MERGE for staging → DWH upsert
/*
MERGE INTO dwh.fact_policies AS target
USING staging.stg_policies   AS source
    ON target.policy_id = source.policy_id
WHEN MATCHED AND target.policy_status != source.status THEN
    UPDATE SET
        policy_status = source.status,
        load_date     = CURRENT_DATE
WHEN NOT MATCHED THEN
    INSERT (policy_id, premium_amount, coverage_amount, policy_status, load_date)
    VALUES (source.policy_id, source.premium_amount, source.coverage_amount,
            source.status, CURRENT_DATE);
*/


-- ============================================================
-- 8. EXPLAIN PLAN DEMO: Index vs Full Scan
-- ============================================================
-- Without index (full scan):
EXPLAIN ANALYZE
SELECT claim_id, fraud_score, claim_status
FROM dwh.fact_claims
WHERE fraud_flag = TRUE AND fraud_score > 0.8;

-- After creating partial index:
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_high_fraud
    ON dwh.fact_claims (fraud_score)
    WHERE fraud_flag = TRUE AND fraud_score > 0.5;

EXPLAIN ANALYZE
SELECT claim_id, fraud_score, claim_status
FROM dwh.fact_claims
WHERE fraud_flag = TRUE AND fraud_score > 0.8;


-- ============================================================
-- 9. Performance Optimization: Materialized View
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.mv_claim_summary AS
SELECT
    d.year,
    d.quarter,
    pt.policy_type_name,
    a.region,
    COUNT(fc.claim_key)                     AS claim_count,
    SUM(fc.claimed_amount)                  AS total_claimed,
    SUM(fc.approved_amount)                 AS total_approved,
    AVG(fc.fraud_score)                     AS avg_fraud_score,
    SUM(fc.fraud_flag::INT)                 AS fraud_count
FROM dwh.fact_claims fc
JOIN dwh.dim_date d         ON fc.claim_date_key   = d.date_key
JOIN dwh.dim_policy_type pt ON fc.policy_type_key  = pt.policy_type_key
JOIN dwh.dim_agents a       ON fc.agent_key        = a.agent_key
GROUP BY d.year, d.quarter, pt.policy_type_name, a.region
WITH DATA;

-- Refresh after ETL load:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.mv_claim_summary;

CREATE UNIQUE INDEX ON dwh.mv_claim_summary
    (year, quarter, policy_type_name, region);

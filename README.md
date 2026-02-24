# 🛡️ Insurance Claim Risk Analytics
### Project 9 — End-to-End Data Engineering Project
**Wipro DAI-DATA Group 1 Training | ITER SOA | February 2026**

---

## 📌 Project Overview

A complete, production-grade **Data Engineering & Analytics** solution for Insurance Claim Risk Analytics built on a modern data stack. The system ingests, transforms, and analyzes data across **1,000 customers**, **1,500 policies**, and **3,000 claims** — providing actionable fraud detection, risk scoring, and business intelligence insights.

---

## 🏗️ Architecture (8-Layer Pipeline)

```
[Data Sources] → [Python ETL] → [Staging Layer] → [Apache Spark]
      ↓                                                   ↓
[Snowflake DWH] ← [Star Schema DWH] ← [SQL Transforms]
      ↓
[RBAC + Masking + RLS] → [Dashboard & BI Reports]
```

---

## 📂 Folder Structure

```
insurance_claim_risk_analytics/
│
├── 📁 data/
│   └── generate_data.py          # Synthetic data generator (1K customers, 3K claims)
│
├── 📁 sql/
│   ├── 01_star_schema_ddl.sql    # Star Schema DDL: dims, facts, indexes, partitions
│   └── 02_advanced_analytics_sql.sql  # CTEs, Window Fns, ROLLUP, CUBE, MERGE
│
├── 📁 etl/
│   └── etl_pipeline.py           # Python ETL: Extract → Transform → Validate → Load
│
├── 📁 spark/
│   └── spark_processing.py       # Spark Batch + Structured Streaming
│
├── 📁 snowflake/
│   └── snowflake_advanced.sql    # Clustering, Time Travel, VARIANT, RBAC, Snowpipe
│
├── 📁 dashboard/
│   └── dashboard.py              # 2-page visualization dashboard (Matplotlib)
│
├── 📁 docs/
│   ├── architecture_diagram.html # Interactive 8-layer architecture diagram
│   └── project_report.docx       # Full project report (11 sections)
│
├── 📁 dashboard_output/          # Generated chart PNGs
│   ├── dashboard_page1_overview.png
│   └── dashboard_page2_risk.png
│
├── requirements.txt
└── README.md
```

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Data Processing | Pandas, NumPy |
| Distributed Computing | Apache Spark 3.x (PySpark) |
| Streaming | Spark Structured Streaming + Kafka |
| Data Warehouse | PostgreSQL (Star Schema) + Snowflake |
| Visualization | Matplotlib |
| Orchestration | Apache Airflow (architecture) |
| Security | Snowflake RBAC, Data Masking, RLS |
| File Formats | CSV, JSON, Parquet, VARIANT |

---

## 🚀 How to Run

### 1. Clone the Repository
```bash
git clone https://github.com/<your-username>/insurance-claim-risk-analytics.git
cd insurance-claim-risk-analytics
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Generate Sample Data
```bash
cd data
python generate_data.py
# Output: data/output/*.csv + claims_semi_structured.json
```

### 4. Run ETL Pipeline
```bash
python etl/etl_pipeline.py --source-dir data/output --output-dir etl_output
# Logs saved to: etl_pipeline.log
# Run stats saved to: etl_output/run_log.json
```

### 5. Run Spark Processing
```bash
# Requires PySpark installed
cd insurance_claim_risk_analytics
python spark/spark_processing.py
# Outputs: spark_output/claims_partitioned/*.parquet
```

### 6. Generate Dashboard
```bash
python dashboard/dashboard.py
# Charts saved to: dashboard_output/
```

### 7. SQL Scripts
- Run `sql/01_star_schema_ddl.sql` on PostgreSQL to create the Star Schema
- Run `sql/02_advanced_analytics_sql.sql` for advanced analytics queries
- Run `snowflake/snowflake_advanced.sql` on Snowflake for cloud DWH features

---

## 📊 Key Features Implemented

### ✅ Module 1 – Data Warehouse Fundamentals
- OLTP vs OLAP design comparison
- ETL → ELT pipeline transition
- Star Schema: 4 dimensions + 2 fact tables
- Staging → Integration → Presentation layers
- Metadata, data dictionary, lineage tracking

### ✅ Module 2 – SQL L2 (Advanced)
- Partitioned tables (range partitioning by year)
- Indexing strategies (B-tree, partial, composite)
- CTEs (multi-level), Recursive CTEs
- Window Functions: RANK, LAG, LEAD, PERCENT_RANK, NTILE, Running SUM
- ROLLUP and CUBE for multi-dimensional aggregation
- MERGE (upsert) for SCD Type 1 updates
- EXPLAIN PLAN / EXPLAIN ANALYZE for query optimization
- Materialized Views with concurrent refresh

### ✅ Module 3 – Python L2
- Decorators: `@timer`, `@retry`, `@audit_log`
- Custom exception hierarchy (`ETLException` → `ExtractionError`, etc.)
- Lambda functions, list comprehensions, generators
- `argparse` CLI with `--source-dir`, `--output-dir`, `--log-level`
- CSV/JSON/Parquet file handling
- Pandas/NumPy for vectorized transformations
- PyODBC/SQLAlchemy-ready loader design

### ✅ Module 4 – Apache Spark L2
- SparkSession with AQE, auto partition coalescing
- DataFrame API + Spark SQL (multi-join analytics)
- RDD: map, reduceByKey, cache, Accumulator, Broadcast variable
- Structured Streaming: file source, watermarks, tumbling window
- Fraud alert stream with alert_level classification
- Performance: broadcast joins, repartition, coalesce, Parquet output
- EXPLAIN (formatted physical plan)

### ✅ Module 5 – Data Storytelling & Visualization
- 10 charts across 2 dashboard pages
- KPI summary cards
- Trend lines, bar charts, histograms, box plots
- Heatmap (Credit × Income fraud rate)
- Bubble scatter plot (Region: amount vs fraud rate)
- Correct chart selection principles applied

### ✅ Module 6 – Snowflake Advanced
- Virtual Warehouses (ETL_WH + ANALYTICS_WH) with auto-suspend
- Clustering keys with automatic re-clustering
- Time Travel: `AT(OFFSET)`, `BEFORE(STATEMENT)`, zero-copy clone
- VARIANT column for semi-structured JSON ingestion
- LATERAL FLATTEN for nested array querying
- Snowpipe: `AUTO_INGEST=TRUE` for event-driven loads
- COPY INTO (bulk load) + Parquet unload
- Materialized Views with auto-refresh
- RBAC: 4-tier role hierarchy
- Dynamic Data Masking: `annual_income`, `credit_score`
- Row-Level Security: region-based access policy
- Secure Data Sharing across accounts

---

## 📈 Results & Key Insights

| Metric | Value |
|---|---|
| Total Claims Processed | 3,000 |
| Fraud Detection Rate | ~12% |
| ETL Pipeline Runtime | 0.2 seconds |
| DQ Pass Rate | 100% |
| Star Schema Tables | 6 (4 dims + 2 facts) |
| SQL Features Demonstrated | 14+ |
| Dashboard Charts | 10 |
| Snowflake Features Used | 12+ |

---

## 📄 Project Report

Full documentation available in [`docs/project_report.docx`](docs/project_report.docx)

Interactive architecture diagram: [`docs/architecture_diagram.html`](docs/architecture_diagram.html)

---

## 👤 Author

**Student Name:** *Reshav Kumar Choudhary*  
**Batch:** Wipro DAI-DATA Group 1  
**Institute:** ITER, SOA University  
**Training Period:** February 2026  
**Project ID:** Project 9 — Insurance Claim Risk Analytics

---

> *Submitted as part of the Wipro DAI-DATA 30 End-to-End Data Engineering & Analytics Projects training program.*

"""
Insurance Claim Risk Analytics
Module 4: Apache Spark – Batch + Streaming Processing
- Spark SQL, DataFrame API, RDD ops
- Catalyst optimizer demo
- Structured Streaming (file source → console sink)
- Performance tuning: partitioning, caching, broadcast joins
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, DateType
)
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("insurance_spark")

ETL_OUTPUT_DIR = "etl_output"
STREAM_INPUT_DIR = "stream_input"
STREAM_CHECKPOINT = "stream_checkpoint"

# ─────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("InsuranceClaimRiskAnalytics")
        .config("spark.sql.adaptive.enabled", "true")              # AQE
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")               # tuned for local
        .config("spark.driver.memory", "2g")
        .config("spark.sql.autoBroadcastJoinThreshold", "10m")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# BATCH: DATA LOADING
# ─────────────────────────────────────────────────────────────
def load_data(spark: SparkSession):
    """Load all dimension and fact tables into DataFrames."""
    claims = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{ETL_OUTPUT_DIR}/dwh_fact_claims.csv")
    )
    policies = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{ETL_OUTPUT_DIR}/dwh_fact_policies.csv")
    )
    customers = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{ETL_OUTPUT_DIR}/dwh_dim_customers.csv")
    )
    agents = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{ETL_OUTPUT_DIR}/dwh_dim_agents.csv")
    )
    policy_types = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{ETL_OUTPUT_DIR}/dwh_dim_policy_type.csv")
    )

    logger.info(f"Claims: {claims.count():,} rows | Policies: {policies.count():,} rows")
    return claims, policies, customers, agents, policy_types


# ─────────────────────────────────────────────────────────────
# BATCH: FRAUD ANALYTICS
# ─────────────────────────────────────────────────────────────
def fraud_risk_analysis(spark, claims, customers, policy_types):
    """Comprehensive fraud analytics using Spark SQL + Window functions."""

    # Register temp views
    claims.createOrReplaceTempView("claims")
    customers.createOrReplaceTempView("customers")
    policy_types.createOrReplaceTempView("policy_types")

    # Broadcast small tables (policy_types) for optimized join
    pt_broadcast = F.broadcast(policy_types)

    # 1. Fraud rate by policy category
    fraud_by_policy = (
        claims.join(pt_broadcast, "policy_type_id", "left")
        .groupBy("category")
        .agg(
            F.count("*").alias("total_claims"),
            F.sum(F.col("fraud_flag").cast("int")).alias("fraud_claims"),
            F.round(
                F.sum(F.col("fraud_flag").cast("int")) * 100.0 / F.count("*"), 2
            ).alias("fraud_rate_pct"),
            F.round(F.avg("fraud_score"), 4).alias("avg_fraud_score"),
            F.round(F.avg("claimed_amount"), 2).alias("avg_claim_amount")
        )
        .orderBy(F.desc("fraud_rate_pct"))
    )
    logger.info("\n=== Fraud Rate by Policy Category ===")
    fraud_by_policy.show()

    # 2. Window: Running fraud count per claim type
    w = Window.partitionBy("claim_type").orderBy("claim_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    fraud_trend = (
        claims
        .withColumn("running_fraud_count",
                    F.sum(F.col("fraud_flag").cast("int")).over(w))
        .withColumn("running_fraud_score_avg",
                    F.round(F.avg("fraud_score").over(w), 4))
        .select("claim_id", "claim_type", "claim_date", "fraud_flag",
                "fraud_score", "running_fraud_count", "running_fraud_score_avg")
    )
    logger.info("\n=== Running Fraud Trend (sample) ===")
    fraud_trend.filter(F.col("claim_type") == "Accident").show(10)

    # 3. Spark SQL: Complex multi-join analytics
    result = spark.sql("""
        SELECT
            c.claim_type,
            COUNT(*)                                           AS claim_count,
            ROUND(AVG(c.claimed_amount), 2)                   AS avg_claimed,
            ROUND(AVG(c.approved_amount), 2)                  AS avg_approved,
            ROUND(AVG(c.fraud_score), 4)                      AS avg_fraud_score,
            SUM(CAST(c.fraud_flag AS INT))                     AS fraud_count,
            ROUND(SUM(CAST(c.fraud_flag AS INT)) * 100.0
                  / COUNT(*), 2)                              AS fraud_pct,
            ROUND(AVG(c.processing_days), 1)                  AS avg_processing_days
        FROM claims c
        GROUP BY c.claim_type
        ORDER BY fraud_pct DESC
    """)
    logger.info("\n=== Claims Analytics by Type ===")
    result.show()
    return fraud_by_policy, fraud_trend


# ─────────────────────────────────────────────────────────────
# BATCH: RISK SCORING WITH RDD
# ─────────────────────────────────────────────────────────────
def rdd_risk_scoring(claims):
    """Demonstrate RDD operations: transformations, actions, caching."""

    # Convert to RDD of dicts
    rdd = claims.rdd.map(lambda row: row.asDict())

    # Cache RDD (persistence)
    rdd.cache()

    # Transformation: compute composite risk score
    def compute_risk(record):
        fraud_score = float(record.get("fraud_score") or 0)
        claimed    = float(record.get("claimed_amount") or 0)
        days       = float(record.get("processing_days") or 0)
        composite  = round(fraud_score * 0.5 + min(claimed / 500000, 1) * 0.3
                           + min(days / 120, 1) * 0.2, 4)
        record["composite_risk"] = composite
        record["risk_label"] = (
            "CRITICAL" if composite >= 0.7 else
            "HIGH"     if composite >= 0.5 else
            "MEDIUM"   if composite >= 0.3 else
            "LOW"
        )
        return record

    scored_rdd = rdd.map(compute_risk)

    # Accumulator: count CRITICAL risk claims
    critical_acc = claims.sparkContext.accumulator(0)

    def count_critical(record):
        if record["risk_label"] == "CRITICAL":
            critical_acc.add(1)
        return record

    scored_rdd = scored_rdd.map(count_critical)

    # Broadcast: lookup for claim type descriptions
    type_descriptions = claims.sparkContext.broadcast({
        "Accident":         "Vehicle/person collision",
        "Theft":            "Stolen property",
        "Natural Disaster": "Weather/earthquake",
        "Medical":          "Health treatment",
        "Fire":             "Fire damage",
        "Flood":            "Water damage",
        "Injury":           "Personal injury",
    })

    def enrich_with_desc(record):
        record["claim_description"] = type_descriptions.value.get(
            record.get("claim_type", ""), "Unknown"
        )
        return record

    final_rdd = scored_rdd.map(enrich_with_desc)

    # Action: collect sample
    sample = final_rdd.take(5)
    logger.info("\n=== RDD Risk Scoring Sample ===")
    for rec in sample:
        logger.info(f"  {rec['claim_id']} | risk={rec['composite_risk']} | label={rec['risk_label']}")

    # Trigger accumulator
    total = final_rdd.count()
    logger.info(f"\nTotal claims: {total:,} | CRITICAL risk: {critical_acc.value:,}")

    # GroupBy: count by risk label
    by_label = (
        final_rdd
        .map(lambda r: (r["risk_label"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )
    logger.info("\n=== Risk Distribution ===")
    for label, cnt in by_label.collect():
        logger.info(f"  {label}: {cnt:,}")

    scored_rdd.unpersist()
    return final_rdd


# ─────────────────────────────────────────────────────────────
# BATCH: PERFORMANCE TUNING DEMO
# ─────────────────────────────────────────────────────────────
def performance_tuning_demo(spark, claims, policies):
    """Show partition tuning, caching, and query plan."""

    # Repartition for better parallelism
    claims_repartitioned = claims.repartition(8, "claim_type")
    logger.info(f"Claims partitions: {claims_repartitioned.rdd.getNumPartitions()}")

    # Cache frequently accessed DataFrame
    claims_repartitioned.cache()
    claims_repartitioned.count()  # trigger materialization

    # EXPLAIN: show physical plan
    logger.info("\n=== Query Execution Plan ===")
    claims_repartitioned.filter(
        F.col("fraud_flag") == True
    ).groupBy("claim_type").count().explain(mode="formatted")

    # Coalesce before writing (reduce output files)
    claims_repartitioned.coalesce(1).write.mode("overwrite").parquet(
        "spark_output/claims_partitioned"
    )
    logger.info("Written Parquet output: spark_output/claims_partitioned/")

    claims_repartitioned.unpersist()


# ─────────────────────────────────────────────────────────────
# STRUCTURED STREAMING: Simulated Real-Time Claim Ingestion
# ─────────────────────────────────────────────────────────────
STREAMING_SCHEMA = StructType([
    StructField("claim_id",       StringType(),  True),
    StructField("policy_id",      StringType(),  True),
    StructField("claim_date",     StringType(),  True),
    StructField("claim_type",     StringType(),  True),
    StructField("claimed_amount", DoubleType(),  True),
    StructField("fraud_score",    DoubleType(),  True),
    StructField("fraud_flag",     BooleanType(), True),
    StructField("status",         StringType(),  True),
])


def create_streaming_data():
    """Write micro-batch CSV files to simulate streaming."""
    import pandas as pd, numpy as np, time, os
    os.makedirs(STREAM_INPUT_DIR, exist_ok=True)

    batch_data = pd.DataFrame({
        "claim_id":       [f"STRM{i:06d}" for i in range(10)],
        "policy_id":      [f"POL{i:06d}" for i in range(10)],
        "claim_date":     ["2025-01-15"] * 10,
        "claim_type":     np.random.choice(["Accident","Theft","Medical","Fire"], 10).tolist(),
        "claimed_amount": np.round(np.random.uniform(5000, 100000, 10), 2).tolist(),
        "fraud_score":    np.round(np.random.uniform(0, 1, 10), 4).tolist(),
        "fraud_flag":     (np.random.rand(10) > 0.85).tolist(),
        "status":         np.random.choice(["Pending","Approved","Rejected"], 10).tolist(),
    })
    batch_data.to_csv(f"{STREAM_INPUT_DIR}/batch_001.csv", index=False)
    logger.info("Streaming micro-batch file created.")


def run_streaming(spark: SparkSession):
    """Run Structured Streaming: file source → fraud alert → console sink."""
    os.makedirs(STREAM_INPUT_DIR, exist_ok=True)
    os.makedirs(STREAM_CHECKPOINT, exist_ok=True)
    create_streaming_data()

    # Read stream from directory
    stream_df = (
        spark.readStream
        .option("header", True)
        .schema(STREAMING_SCHEMA)
        .csv(STREAM_INPUT_DIR)
    )

    # Watermark + aggregation for late data handling
    fraud_alerts = (
        stream_df
        .withWatermark("claim_date", "10 minutes")
        .withColumn("alert_level",
            F.when(F.col("fraud_score") >= 0.8, "🔴 CRITICAL")
            .when(F.col("fraud_score") >= 0.5, "🟡 HIGH")
            .otherwise("🟢 NORMAL")
        )
        .filter(F.col("fraud_flag") == True)
        .select("claim_id", "claim_type", "claimed_amount",
                "fraud_score", "alert_level", "status")
    )

    # Aggregated stream: fraud count by claim type (tumbling trigger)
    agg_stream = (
        stream_df
        .groupBy("claim_type")
        .agg(
            F.count("*").alias("claim_count"),
            F.sum(F.col("fraud_flag").cast("int")).alias("fraud_count"),
            F.round(F.avg("fraud_score"), 4).alias("avg_fraud_score")
        )
    )

    # Write to console (demo) + parquet sink
    query1 = (
        fraud_alerts.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{STREAM_CHECKPOINT}/alerts")
        .trigger(once=True)
        .start()
    )

    query2 = (
        agg_stream.writeStream
        .outputMode("complete")
        .format("console")
        .option("checkpointLocation", f"{STREAM_CHECKPOINT}/agg")
        .trigger(once=True)
        .start()
    )

    query1.awaitTermination(30)
    query2.awaitTermination(30)
    logger.info("Streaming queries completed.")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    os.makedirs("spark_output", exist_ok=True)

    logger.info("\n" + "="*60)
    logger.info("Spark Batch Processing – Insurance Claim Analytics")
    logger.info("="*60)

    claims, policies, customers, agents, policy_types = load_data(spark)

    # Batch analytics
    fraud_by_policy, fraud_trend = fraud_risk_analysis(
        spark, claims, customers, policy_types
    )

    # RDD-based risk scoring
    rdd_risk_scoring(claims)

    # Performance tuning
    performance_tuning_demo(spark, claims, policies)

    # Streaming
    logger.info("\n" + "="*60)
    logger.info("Spark Structured Streaming – Real-Time Fraud Alerts")
    logger.info("="*60)
    run_streaming(spark)

    spark.stop()
    logger.info("✅ All Spark jobs completed.")


if __name__ == "__main__":
    main()

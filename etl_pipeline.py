"""
Insurance Claim Risk Analytics
Module 3: Python ETL/ELT Pipeline
- Extract from CSVs / API (simulated)
- Transform with Pandas / NumPy
- Load to target (PostgreSQL / Snowflake)
- Supports CLI arguments (argparse)
- Includes decorators, logging, exception handling
"""

import os
import sys
import json
import logging
import argparse
import time
from datetime import datetime, date
from functools import wraps
from typing import Optional, Dict, Any

import pandas as pd
import numpy as np

# ──────────────────────────────────────────────
# LOGGING SETUP
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger("insurance_etl")


# ──────────────────────────────────────────────
# DECORATORS
# ──────────────────────────────────────────────
def timer(func):
    """Decorator: log execution time of any ETL step."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        logger.info(f"▶ Starting: {func.__name__}")
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.info(f"✔ Completed: {func.__name__} in {elapsed:.2f}s")
        return result
    return wrapper


def retry(max_retries: int = 3, delay: float = 2.0):
    """Decorator: retry on transient failures."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/{max_retries} failed: {e}")
                    if attempt == max_retries:
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator


def audit_log(func):
    """Decorator: write audit record to log."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        logger.info(f"[AUDIT] {func.__name__} | rows={getattr(result, 'shape', (None,))[0]} | ts={datetime.utcnow().isoformat()}")
        return result
    return wrapper


# ──────────────────────────────────────────────
# CUSTOM EXCEPTIONS
# ──────────────────────────────────────────────
class ETLException(Exception):
    """Base ETL exception."""

class ExtractionError(ETLException):
    """Raised when data extraction fails."""

class TransformationError(ETLException):
    """Raised when data transformation fails."""

class LoadError(ETLException):
    """Raised when data loading fails."""

class ValidationError(ETLException):
    """Raised when data quality checks fail."""


# ──────────────────────────────────────────────
# EXTRACTOR
# ──────────────────────────────────────────────
class DataExtractor:
    """Handles extraction from CSV / JSON / API sources."""

    def __init__(self, source_dir: str):
        self.source_dir = source_dir

    @timer
    @retry(max_retries=3)
    @audit_log
    def extract_csv(self, filename: str, **kwargs) -> pd.DataFrame:
        path = os.path.join(self.source_dir, filename)
        if not os.path.exists(path):
            raise ExtractionError(f"File not found: {path}")
        df = pd.read_csv(path, **kwargs)
        logger.info(f"Extracted {len(df):,} rows from {filename}")
        return df

    @timer
    def extract_json(self, filename: str) -> list:
        path = os.path.join(self.source_dir, filename)
        with open(path) as f:
            data = json.load(f)
        logger.info(f"Extracted {len(data):,} JSON records from {filename}")
        return data


# ──────────────────────────────────────────────
# TRANSFORMER
# ──────────────────────────────────────────────
class DataTransformer:
    """Cleans, enriches, and restructures raw data."""

    # --- Customers ---
    @timer
    @audit_log
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.copy()
            # Age bands
            df['age_band'] = pd.cut(
                df['age'],
                bins=[0, 25, 35, 45, 55, 65, 120],
                labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+']
            ).astype(str)
            # Income bands
            df['income_band'] = pd.cut(
                df['annual_income'],
                bins=[0, 40000, 80000, 150000, float('inf')],
                labels=['Low', 'Medium', 'High', 'Very High']
            ).astype(str)
            # Credit bands
            df['credit_band'] = pd.cut(
                df['credit_score'],
                bins=[0, 579, 669, 739, 799, 850],
                labels=['Poor', 'Fair', 'Good', 'Very Good', 'Excellent']
            ).astype(str)
            # Nulls
            df['gender'].fillna('Unknown', inplace=True)
            df['marital_status'].fillna('Unknown', inplace=True)
            # Surrogate key placeholder
            df.insert(0, 'customer_key', range(1, len(df) + 1))
            df['is_current'] = True
            df['eff_start_date'] = date.today().isoformat()
            df['eff_end_date'] = '9999-12-31'
            df['last_updated'] = datetime.utcnow().isoformat()
            return df
        except Exception as e:
            raise TransformationError(f"Customer transform failed: {e}") from e

    # --- Claims ---
    @timer
    @audit_log
    def transform_claims(self, df: pd.DataFrame, policies_df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.copy()
            # Payout ratio
            df['payout_ratio'] = np.where(
                df['claimed_amount'] > 0,
                df['approved_amount'] / df['claimed_amount'],
                0.0
            ).round(4)
            # Rejected amount
            df['rejected_amount'] = (df['claimed_amount'] - df['approved_amount']).round(2)
            # Risk score: composite
            df['risk_score'] = (
                df['fraud_score'] * 0.5 +
                (df['claimed_amount'] / df['claimed_amount'].max()) * 0.3 +
                (df['processing_days'] / df['processing_days'].max()) * 0.2
            ).round(4)
            # Join policy risk score
            df = df.merge(
                policies_df[['policy_id', 'risk_score']].rename(
                    columns={'risk_score': 'policy_risk_score'}
                ),
                on='policy_id', how='left'
            )
            # Date key
            df['date_id'] = pd.to_datetime(df['claim_date']).dt.strftime('%Y%m%d').astype(int)
            df['fraud_flag'] = df['fraud_flag'].astype(bool)
            df['load_date'] = date.today().isoformat()
            return df
        except Exception as e:
            raise TransformationError(f"Claims transform failed: {e}") from e

    # --- Policies ---
    @timer
    @audit_log
    def transform_policies(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.copy()
            df['start_date'] = pd.to_datetime(df['start_date'])
            df['end_date']   = pd.to_datetime(df['end_date'])
            df['policy_duration_days'] = (df['end_date'] - df['start_date']).dt.days
            df['start_date_key'] = df['start_date'].dt.strftime('%Y%m%d').astype(int)
            df['end_date_key']   = df['end_date'].dt.strftime('%Y%m%d').astype(int)
            df['load_date'] = date.today().isoformat()
            return df
        except Exception as e:
            raise TransformationError(f"Policy transform failed: {e}") from e


# ──────────────────────────────────────────────
# DATA QUALITY VALIDATOR
# ──────────────────────────────────────────────
class DataQualityChecker:
    """Validates data quality before loading."""

    def __init__(self, fail_threshold: float = 0.05):
        self.fail_threshold = fail_threshold   # 5% null tolerance

    def check(self, df: pd.DataFrame, table_name: str, key_cols: list) -> Dict[str, Any]:
        report = {"table": table_name, "total_rows": len(df), "issues": []}

        # 1. Null check on key columns
        for col in key_cols:
            null_pct = df[col].isna().mean()
            if null_pct > self.fail_threshold:
                report["issues"].append(
                    f"Column '{col}' has {null_pct:.1%} nulls (threshold={self.fail_threshold:.0%})"
                )

        # 2. Duplicate key check
        dup_count = df.duplicated(subset=key_cols).sum()
        if dup_count > 0:
            report["issues"].append(f"Found {dup_count} duplicate rows on {key_cols}")

        # 3. Range checks
        if 'fraud_score' in df.columns:
            out_of_range = ((df['fraud_score'] < 0) | (df['fraud_score'] > 1)).sum()
            if out_of_range:
                report["issues"].append(f"fraud_score out of [0,1]: {out_of_range} rows")

        if 'payout_ratio' in df.columns:
            negatives = (df['payout_ratio'] < 0).sum()
            if negatives:
                report["issues"].append(f"Negative payout_ratio: {negatives} rows")

        report["passed"] = len(report["issues"]) == 0
        return report


# ──────────────────────────────────────────────
# LOADER
# ──────────────────────────────────────────────
class DataLoader:
    """Loads transformed data to target (CSV for demo; extend for DB)."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    @timer
    def load_to_csv(self, df: pd.DataFrame, table_name: str) -> None:
        """Simulate load; in production use SQLAlchemy / Snowflake connector."""
        path = os.path.join(self.output_dir, f"{table_name}.csv")
        df.to_csv(path, index=False)
        logger.info(f"Loaded {len(df):,} rows → {path}")

    @timer
    def load_batch(self, dfs: Dict[str, pd.DataFrame]) -> None:
        for table, df in dfs.items():
            self.load_to_csv(df, table)


# ──────────────────────────────────────────────
# PIPELINE ORCHESTRATOR
# ──────────────────────────────────────────────
class InsuranceETLPipeline:
    """End-to-end ETL pipeline orchestrator."""

    def __init__(self, source_dir: str, output_dir: str):
        self.extractor  = DataExtractor(source_dir)
        self.transformer = DataTransformer()
        self.validator   = DataQualityChecker()
        self.loader      = DataLoader(output_dir)
        self.run_stats   = {}

    def run(self) -> None:
        logger.info("=" * 60)
        logger.info("Insurance ETL Pipeline – START")
        logger.info("=" * 60)
        start_ts = datetime.utcnow()

        try:
            # EXTRACT
            raw_customers    = self.extractor.extract_csv("dim_customers.csv")
            raw_policies     = self.extractor.extract_csv("fact_policies.csv")
            raw_claims       = self.extractor.extract_csv("fact_claims.csv")
            raw_agents       = self.extractor.extract_csv("dim_agents.csv")
            raw_policy_types = self.extractor.extract_csv("dim_policy_types.csv")
            raw_dates        = self.extractor.extract_csv("dim_date.csv")

            # TRANSFORM
            customers    = self.transformer.transform_customers(raw_customers)
            policies     = self.transformer.transform_policies(raw_policies)
            claims       = self.transformer.transform_claims(raw_claims, policies)

            # VALIDATE
            for df, name, keys in [
                (customers, "dim_customers",  ["customer_id"]),
                (policies,  "fact_policies",  ["policy_id"]),
                (claims,    "fact_claims",    ["claim_id"]),
            ]:
                report = self.validator.check(df, name, keys)
                if not report["passed"]:
                    raise ValidationError(f"DQ failed for {name}: {report['issues']}")
                logger.info(f"✔ DQ passed: {name} ({report['total_rows']:,} rows)")

            # LOAD
            self.loader.load_batch({
                "dwh_dim_customers":   customers,
                "dwh_fact_policies":   policies,
                "dwh_fact_claims":     claims,
                "dwh_dim_agents":      raw_agents,
                "dwh_dim_policy_type": raw_policy_types,
                "dwh_dim_date":        raw_dates,
            })

            elapsed = (datetime.utcnow() - start_ts).total_seconds()
            self.run_stats = {
                "status":        "SUCCESS",
                "run_timestamp": start_ts.isoformat(),
                "duration_sec":  round(elapsed, 2),
                "rows_loaded": {
                    "customers":   len(customers),
                    "policies":    len(policies),
                    "claims":      len(claims),
                }
            }
            logger.info(f"\n{'='*60}\nPipeline completed in {elapsed:.1f}s\n{'='*60}")

        except (ExtractionError, TransformationError, ValidationError, LoadError) as e:
            logger.error(f"Pipeline FAILED: {e}")
            self.run_stats = {"status": "FAILED", "error": str(e)}
            raise
        finally:
            # Write run log
            with open(os.path.join(self.loader.output_dir, "run_log.json"), "w") as f:
                json.dump(self.run_stats, f, indent=2)


# ──────────────────────────────────────────────
# CLI ENTRY POINT
# ──────────────────────────────────────────────
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Insurance Claim Risk Analytics – ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--source-dir", default="data/output",
        help="Directory containing raw source CSVs"
    )
    parser.add_argument(
        "--output-dir", default="etl_output",
        help="Directory for transformed output files"
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity"
    )
    return parser


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    pipeline = InsuranceETLPipeline(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
    )
    pipeline.run()

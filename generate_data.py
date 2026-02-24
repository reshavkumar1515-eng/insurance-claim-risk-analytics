"""
Insurance Claim Risk Analytics - Sample Data Generator
Generates realistic insurance claim data for the project
"""
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

N_CUSTOMERS = 1000
N_POLICIES = 1500
N_CLAIMS = 3000
N_AGENTS = 50
N_REGIONS = 10

# --- Dimension: Customers ---
genders = ['Male', 'Female', 'Other']
marital_statuses = ['Single', 'Married', 'Divorced', 'Widowed']
occupations = ['Employed', 'Self-Employed', 'Unemployed', 'Retired', 'Student']

customers = pd.DataFrame({
    'customer_id': [f'CUST{str(i).zfill(5)}' for i in range(1, N_CUSTOMERS + 1)],
    'first_name': [f'FirstName{i}' for i in range(1, N_CUSTOMERS + 1)],
    'last_name': [f'LastName{i}' for i in range(1, N_CUSTOMERS + 1)],
    'age': np.random.randint(18, 75, N_CUSTOMERS),
    'gender': np.random.choice(genders, N_CUSTOMERS),
    'marital_status': np.random.choice(marital_statuses, N_CUSTOMERS),
    'occupation': np.random.choice(occupations, N_CUSTOMERS),
    'annual_income': np.random.randint(20000, 200000, N_CUSTOMERS),
    'credit_score': np.random.randint(300, 850, N_CUSTOMERS),
    'city': np.random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Hyderabad',
                              'Kolkata', 'Pune', 'Ahmedabad', 'Jaipur', 'Surat'], N_CUSTOMERS),
    'state': np.random.choice(['Maharashtra', 'Delhi', 'Karnataka', 'Tamil Nadu', 'Telangana',
                               'West Bengal', 'Gujarat', 'Rajasthan'], N_CUSTOMERS),
    'created_date': [
        (datetime(2018, 1, 1) + timedelta(days=random.randint(0, 1800))).strftime('%Y-%m-%d')
        for _ in range(N_CUSTOMERS)
    ]
})

# --- Dimension: Policy Types ---
policy_types = pd.DataFrame({
    'policy_type_id': ['PT001', 'PT002', 'PT003', 'PT004', 'PT005'],
    'policy_type_name': ['Auto Insurance', 'Health Insurance', 'Home Insurance',
                         'Life Insurance', 'Travel Insurance'],
    'category': ['Vehicle', 'Health', 'Property', 'Life', 'Travel'],
    'base_premium_rate': [0.05, 0.08, 0.03, 0.02, 0.01],
    'max_coverage_amount': [500000, 1000000, 2000000, 5000000, 100000]
})

# --- Dimension: Agents ---
agents = pd.DataFrame({
    'agent_id': [f'AGT{str(i).zfill(4)}' for i in range(1, N_AGENTS + 1)],
    'agent_name': [f'Agent {i}' for i in range(1, N_AGENTS + 1)],
    'region': np.random.choice(['North', 'South', 'East', 'West', 'Central',
                                'Northeast', 'Northwest', 'Southeast', 'Southwest', 'Central East'], N_AGENTS),
    'experience_years': np.random.randint(1, 20, N_AGENTS),
    'performance_rating': np.round(np.random.uniform(2.5, 5.0, N_AGENTS), 1)
})

# --- Dimension: Date ---
start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 12, 31)
date_range = pd.date_range(start_date, end_date, freq='D')

date_dim = pd.DataFrame({
    'date_id': date_range.strftime('%Y%m%d').astype(int),
    'full_date': date_range.strftime('%Y-%m-%d'),
    'year': date_range.year,
    'quarter': date_range.quarter,
    'month': date_range.month,
    'month_name': date_range.strftime('%B'),
    'week': date_range.isocalendar().week,
    'day_of_week': date_range.dayofweek,
    'day_name': date_range.strftime('%A'),
    'is_weekend': (date_range.dayofweek >= 5).astype(int),
    'is_holiday': np.random.choice([0, 1], len(date_range), p=[0.97, 0.03])
})

# --- Fact: Policies ---
policy_start_dates = [
    (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460)))
    for _ in range(N_POLICIES)
]

policies = pd.DataFrame({
    'policy_id': [f'POL{str(i).zfill(6)}' for i in range(1, N_POLICIES + 1)],
    'customer_id': np.random.choice(customers['customer_id'], N_POLICIES),
    'policy_type_id': np.random.choice(policy_types['policy_type_id'], N_POLICIES),
    'agent_id': np.random.choice(agents['agent_id'], N_POLICIES),
    'start_date': [d.strftime('%Y-%m-%d') for d in policy_start_dates],
    'end_date': [(d + timedelta(days=365)).strftime('%Y-%m-%d') for d in policy_start_dates],
    'premium_amount': np.round(np.random.uniform(5000, 50000, N_POLICIES), 2),
    'coverage_amount': np.random.randint(100000, 2000000, N_POLICIES),
    'deductible_amount': np.random.choice([5000, 10000, 15000, 25000], N_POLICIES),
    'status': np.random.choice(['Active', 'Expired', 'Cancelled', 'Lapsed'], N_POLICIES,
                               p=[0.6, 0.25, 0.1, 0.05]),
    'risk_score': np.round(np.random.uniform(1.0, 10.0, N_POLICIES), 2)
})

# --- Fact: Claims ---
claim_statuses = ['Approved', 'Rejected', 'Pending', 'Under Investigation', 'Settled']
claim_types = ['Accident', 'Theft', 'Natural Disaster', 'Medical', 'Fire', 'Flood', 'Injury']
fraud_flags = np.random.choice([0, 1], N_CLAIMS, p=[0.88, 0.12])

claim_dates = [
    (datetime(2020, 6, 1) + timedelta(days=random.randint(0, 1600)))
    for _ in range(N_CLAIMS)
]

claims = pd.DataFrame({
    'claim_id': [f'CLM{str(i).zfill(7)}' for i in range(1, N_CLAIMS + 1)],
    'policy_id': np.random.choice(policies['policy_id'], N_CLAIMS),
    'claim_date': [d.strftime('%Y-%m-%d') for d in claim_dates],
    'claim_type': np.random.choice(claim_types, N_CLAIMS),
    'claimed_amount': np.round(np.random.uniform(5000, 500000, N_CLAIMS), 2),
    'approved_amount': np.round(np.random.uniform(0, 400000, N_CLAIMS), 2),
    'status': np.random.choice(claim_statuses, N_CLAIMS, p=[0.45, 0.20, 0.15, 0.10, 0.10]),
    'fraud_flag': fraud_flags,
    'fraud_score': np.round(np.random.uniform(0, 1, N_CLAIMS), 4),
    'processing_days': np.random.randint(1, 120, N_CLAIMS),
    'adjuster_notes': [f'Claim {i} - Standard review completed.' for i in range(1, N_CLAIMS + 1)],
    'date_id': [d.strftime('%Y%m%d') for d in claim_dates]
})

# --- Semi-Structured: Claims JSON (for Snowflake VARIANT demo) ---
claims_json = []
for _, row in claims.head(100).iterrows():
    claims_json.append({
        "claim_id": row['claim_id'],
        "metadata": {
            "claim_type": row['claim_type'],
            "status": row['status'],
            "fraud_indicators": {
                "is_fraud": bool(row['fraud_flag']),
                "fraud_score": float(row['fraud_score']),
                "flags": random.sample(
                    ['late_filing', 'excessive_amount', 'repeat_claimant',
                     'inconsistent_docs', 'suspicious_timing'], 
                    k=random.randint(0, 3)
                )
            }
        },
        "financial": {
            "claimed_amount": float(row['claimed_amount']),
            "approved_amount": float(row['approved_amount']),
            "processing_days": int(row['processing_days'])
        }
    })

# Save all data
os.makedirs('output', exist_ok=True)
customers.to_csv('output/dim_customers.csv', index=False)
policy_types.to_csv('output/dim_policy_types.csv', index=False)
agents.to_csv('output/dim_agents.csv', index=False)
date_dim.to_csv('output/dim_date.csv', index=False)
policies.to_csv('output/fact_policies.csv', index=False)
claims.to_csv('output/fact_claims.csv', index=False)

with open('output/claims_semi_structured.json', 'w') as f:
    json.dump(claims_json, f, indent=2)

print("✅ Data generation complete!")
print(f"   Customers: {len(customers)}")
print(f"   Policy Types: {len(policy_types)}")
print(f"   Agents: {len(agents)}")
print(f"   Date Records: {len(date_dim)}")
print(f"   Policies: {len(policies)}")
print(f"   Claims: {len(claims)}")
print(f"   JSON Claims (semi-structured): {len(claims_json)}")

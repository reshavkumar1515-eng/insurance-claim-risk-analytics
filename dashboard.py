"""
Insurance Claim Risk Analytics
Module 5: Interactive Data Visualization Dashboard
- Fraud risk overview
- Claims trend analysis
- Geospatial risk heatmap
- Policy performance
- Built with: Pandas + Matplotlib + HTML output
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.gridspec as gridspec
from matplotlib.patches import FancyBboxPatch
import warnings
import os

warnings.filterwarnings('ignore')
np.random.seed(42)

ETL_DIR = "etl_output"
OUT_DIR = "dashboard_output"
os.makedirs(OUT_DIR, exist_ok=True)

COLORS = {
    'primary':   '#1B4F72',
    'danger':    '#E74C3C',
    'warning':   '#F39C12',
    'success':   '#27AE60',
    'info':      '#2E86AB',
    'light':     '#F8F9FA',
    'dark':      '#2C3E50',
    'accent':    '#8E44AD',
}
PALETTE = ['#1B4F72', '#2E86AB', '#27AE60', '#F39C12', '#E74C3C',
           '#8E44AD', '#17A589', '#D35400', '#7F8C8D', '#2ECC71']


# ─────────────────────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────────────────────
def load_data():
    claims    = pd.read_csv(f"{ETL_DIR}/dwh_fact_claims.csv")
    policies  = pd.read_csv(f"{ETL_DIR}/dwh_fact_policies.csv")
    customers = pd.read_csv(f"{ETL_DIR}/dwh_dim_customers.csv")
    agents    = pd.read_csv(f"{ETL_DIR}/dwh_dim_agents.csv")
    pt        = pd.read_csv(f"{ETL_DIR}/dwh_dim_policy_type.csv")
    dates     = pd.read_csv(f"{ETL_DIR}/dwh_dim_date.csv")

    claims['claim_date'] = pd.to_datetime(claims['claim_date'])
    claims['year']  = claims['claim_date'].dt.year
    claims['month'] = claims['claim_date'].dt.month
    claims['quarter'] = claims['claim_date'].dt.quarter

    # Join via policies to get policy_type_id, agent_id, customer_id
    pol_slim = policies[['policy_id', 'policy_type_id', 'agent_id', 'customer_id']]
    claims = claims.merge(pol_slim, on='policy_id', how='left')

    # Join policy type
    claims = claims.merge(
        pt[['policy_type_id', 'policy_type_name', 'category']],
        on='policy_type_id', how='left'
    )
    # Join agent region
    claims = claims.merge(
        agents[['agent_id', 'region']], on='agent_id', how='left'
    )
    # Join customer demographics
    claims = claims.merge(
        customers[['customer_id', 'age_band', 'income_band', 'credit_band',
                   'gender', 'city', 'state']],
        on='customer_id', how='left'
    )
    # add a claim_key surrogate for counts
    claims['claim_key'] = range(1, len(claims)+1)
    return claims, policies, customers, agents, pt


# ─────────────────────────────────────────────────────────────
# DASHBOARD PAGE 1: KPI Summary + Fraud Overview
# ─────────────────────────────────────────────────────────────
def plot_page1_overview(claims, policies):
    fig = plt.figure(figsize=(20, 14))
    fig.patch.set_facecolor(COLORS['dark'])
    gs = gridspec.GridSpec(3, 4, figure=fig, hspace=0.45, wspace=0.35)

    # ── Title ──
    fig.text(0.5, 0.97, '🛡  Insurance Claim Risk Analytics Dashboard',
             ha='center', va='top', fontsize=22, fontweight='bold',
             color='white', family='DejaVu Sans')
    fig.text(0.5, 0.935, 'Wipro DAI-DATA | Project 9 | Powered by DWH + Spark + Snowflake',
             ha='center', va='top', fontsize=11, color='#AAB7B8')

    # ── KPI Cards ──
    kpis = [
        ("Total Claims",      f"{len(claims):,}",             COLORS['info'],    "📋"),
        ("Total Claimed",     f"₹{claims['claimed_amount'].sum()/1e6:.1f}M",
                                                               COLORS['primary'], "💰"),
        ("Fraud Claims",      f"{claims['fraud_flag'].sum():,}",COLORS['danger'], "🚨"),
        ("Fraud Rate",        f"{claims['fraud_flag'].mean()*100:.1f}%",
                                                               COLORS['warning'], "⚠️"),
    ]
    for i, (label, value, color, icon) in enumerate(kpis):
        ax = fig.add_subplot(gs[0, i])
        ax.set_facecolor(color)
        ax.set_xlim(0, 1); ax.set_ylim(0, 1)
        ax.axis('off')
        ax.text(0.5, 0.75, icon, ha='center', va='center', fontsize=26, color='white')
        ax.text(0.5, 0.42, value, ha='center', va='center',
                fontsize=20, fontweight='bold', color='white')
        ax.text(0.5, 0.15, label, ha='center', va='center',
                fontsize=10, color='#FDFEFE', alpha=0.9)

    # ── Chart 1: Claims by Type (Horizontal Bar) ──
    ax2 = fig.add_subplot(gs[1, :2])
    ax2.set_facecolor('#1A252F')
    by_type = (claims.groupby('claim_type')
               .agg(total=('claim_key','count'),
                    fraud_cnt=('fraud_flag','sum'))
               .assign(fraud_rate=lambda x: x.fraud_cnt/x.total*100)
               .sort_values('total'))
    bars = ax2.barh(by_type.index, by_type['total'],
                    color=PALETTE[:len(by_type)], alpha=0.85, edgecolor='none')
    for bar, val in zip(bars, by_type['total']):
        ax2.text(bar.get_width() + 5, bar.get_y() + bar.get_height()/2,
                 f'{val:,}', va='center', fontsize=9, color='white')
    ax2.set_title('Claims by Type', color='white', fontsize=13, pad=10, fontweight='bold')
    ax2.tick_params(colors='white'); ax2.xaxis.label.set_color('white')
    ax2.spines['bottom'].set_color('#4A4A4A'); ax2.spines['left'].set_color('#4A4A4A')
    [ax2.spines[s].set_visible(False) for s in ['top', 'right']]

    # ── Chart 2: Fraud Rate by Policy Type (Donut-like bar) ──
    ax3 = fig.add_subplot(gs[1, 2:])
    ax3.set_facecolor('#1A252F')
    by_policy = (claims.groupby('category')
                 .agg(fraud_rate=('fraud_flag', lambda x: x.mean()*100))
                 .sort_values('fraud_rate', ascending=False))
    colors_pol = [COLORS['danger'] if v > 13 else COLORS['warning'] if v > 11
                  else COLORS['success'] for v in by_policy['fraud_rate']]
    bars3 = ax3.bar(by_policy.index, by_policy['fraud_rate'],
                    color=colors_pol, alpha=0.9, width=0.6, edgecolor='none')
    for bar, val in zip(bars3, by_policy['fraud_rate']):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                 f'{val:.1f}%', ha='center', fontsize=10, color='white', fontweight='bold')
    ax3.set_title('Fraud Rate by Policy Category', color='white',
                  fontsize=13, pad=10, fontweight='bold')
    ax3.axhline(y=claims['fraud_flag'].mean()*100, color='#F39C12',
                linestyle='--', linewidth=1.5, alpha=0.8)
    ax3.set_ylabel('Fraud Rate %', color='white')
    ax3.tick_params(colors='white')
    [ax3.spines[s].set_visible(False) for s in ['top', 'right']]
    ax3.spines['bottom'].set_color('#4A4A4A'); ax3.spines['left'].set_color('#4A4A4A')

    # ── Chart 3: Monthly Claims Trend ──
    ax4 = fig.add_subplot(gs[2, :3])
    ax4.set_facecolor('#1A252F')
    monthly = (claims[claims['year'].isin([2022, 2023, 2024])]
               .groupby(['year', 'month'])
               .agg(count=('claim_key','count'),
                    fraud=('fraud_flag','sum'))
               .reset_index())
    colors_year = {'2022': COLORS['info'], '2023': COLORS['success'], '2024': COLORS['warning']}
    for yr in [2022, 2023, 2024]:
        subset = monthly[monthly['year'] == yr]
        ax4.plot(subset['month'], subset['count'],
                 marker='o', markersize=5, linewidth=2,
                 color=colors_year[str(yr)], label=str(yr))
        ax4.fill_between(subset['month'], subset['count'],
                         alpha=0.08, color=colors_year[str(yr)])
    ax4.set_title('Monthly Claims Trend (2022–2024)', color='white',
                  fontsize=13, pad=10, fontweight='bold')
    ax4.set_xlabel('Month', color='white'); ax4.set_ylabel('Claim Count', color='white')
    ax4.legend(facecolor='#2C3E50', labelcolor='white', fontsize=10)
    ax4.set_xticks(range(1,13))
    ax4.set_xticklabels(['Jan','Feb','Mar','Apr','May','Jun',
                          'Jul','Aug','Sep','Oct','Nov','Dec'], color='white')
    ax4.tick_params(colors='white')
    [ax4.spines[s].set_visible(False) for s in ['top', 'right']]
    ax4.spines['bottom'].set_color('#4A4A4A'); ax4.spines['left'].set_color('#4A4A4A')

    # ── Chart 4: Fraud Score Distribution ──
    ax5 = fig.add_subplot(gs[2, 3])
    ax5.set_facecolor('#1A252F')
    fraud_claims   = claims[claims['fraud_flag'] == True]['fraud_score']
    clean_claims   = claims[claims['fraud_flag'] == False]['fraud_score']
    ax5.hist(clean_claims, bins=25, alpha=0.6, color=COLORS['success'],
             label='Legitimate', density=True, edgecolor='none')
    ax5.hist(fraud_claims,  bins=25, alpha=0.7, color=COLORS['danger'],
             label='Fraud', density=True, edgecolor='none')
    ax5.set_title('Fraud Score Distribution', color='white',
                  fontsize=11, pad=8, fontweight='bold')
    ax5.set_xlabel('Fraud Score', color='white')
    ax5.legend(facecolor='#2C3E50', labelcolor='white', fontsize=8)
    ax5.tick_params(colors='white')
    [ax5.spines[s].set_visible(False) for s in ['top', 'right']]
    ax5.spines['bottom'].set_color('#4A4A4A'); ax5.spines['left'].set_color('#4A4A4A')

    plt.savefig(f"{OUT_DIR}/dashboard_page1_overview.png",
                dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close()
    print(f"✅ Saved: dashboard_page1_overview.png")


# ─────────────────────────────────────────────────────────────
# DASHBOARD PAGE 2: Customer Risk & Agent Performance
# ─────────────────────────────────────────────────────────────
def plot_page2_risk(claims):
    fig = plt.figure(figsize=(20, 12))
    fig.patch.set_facecolor(COLORS['dark'])
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)

    fig.text(0.5, 0.97, '📊 Customer Risk & Agent Performance Analysis',
             ha='center', va='top', fontsize=20, fontweight='bold', color='white')

    # ── 1. Fraud rate by Age Band ──
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor('#1A252F')
    age_data = (claims.groupby('age_band')
                .agg(fraud_rate=('fraud_flag', lambda x: x.mean()*100),
                     count=('claim_key', 'count'))
                .reindex(['18-25','26-35','36-45','46-55','56-65','65+']))
    age_data['fraud_rate'].plot(kind='bar', ax=ax1, color=PALETTE[:6],
                                 alpha=0.9, edgecolor='none')
    ax1.set_title('Fraud Rate by Age Band', color='white', fontweight='bold')
    ax1.set_xlabel('Age Band', color='white'); ax1.set_ylabel('Fraud Rate %', color='white')
    ax1.tick_params(colors='white', axis='both'); ax1.tick_params(axis='x', rotation=30)
    [ax1.spines[s].set_visible(False) for s in ['top', 'right']]
    ax1.spines['bottom'].set_color('#4A4A4A'); ax1.spines['left'].set_color('#4A4A4A')

    # ── 2. Heatmap: Fraud Rate × Credit Band × Income Band ──
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor('#1A252F')
    pivot = (claims.groupby(['credit_band', 'income_band'])
             ['fraud_flag'].mean() * 100).unstack(fill_value=0)
    im = ax2.imshow(pivot.values, cmap='RdYlGn_r', aspect='auto', vmin=0, vmax=25)
    ax2.set_xticks(range(len(pivot.columns))); ax2.set_xticklabels(pivot.columns,
        rotation=30, ha='right', color='white', fontsize=8)
    ax2.set_yticks(range(len(pivot.index))); ax2.set_yticklabels(pivot.index, color='white', fontsize=8)
    plt.colorbar(im, ax=ax2, label='Fraud Rate %')
    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            ax2.text(j, i, f'{pivot.values[i,j]:.0f}%',
                     ha='center', va='center', fontsize=8, color='white', fontweight='bold')
    ax2.set_title('Fraud Rate: Credit × Income Band', color='white', fontweight='bold')

    # ── 3. Region Performance ──
    ax3 = fig.add_subplot(gs[0, 2])
    ax3.set_facecolor('#1A252F')
    region_data = (claims.groupby('region')
                   .agg(total=('claim_key','count'),
                        fraud=('fraud_flag','sum'),
                        avg_amount=('claimed_amount','mean'))
                   .assign(fraud_rate=lambda x: x.fraud/x.total*100)
                   .sort_values('fraud_rate', ascending=False))
    scatter = ax3.scatter(region_data['avg_amount'], region_data['fraud_rate'],
                          s=region_data['total']/3, c=PALETTE[:len(region_data)],
                          alpha=0.8, edgecolors='white', linewidths=0.5)
    for i, (idx, row) in enumerate(region_data.iterrows()):
        ax3.annotate(idx, (row['avg_amount'], row['fraud_rate']),
                     textcoords='offset points', xytext=(5, 3),
                     fontsize=7, color='white')
    ax3.set_xlabel('Avg Claim Amount (₹)', color='white')
    ax3.set_ylabel('Fraud Rate %', color='white')
    ax3.set_title('Region: Claim Amount vs Fraud Rate\n(bubble size = volume)',
                  color='white', fontweight='bold', fontsize=10)
    ax3.tick_params(colors='white')
    [ax3.spines[s].set_visible(False) for s in ['top', 'right']]
    ax3.spines['bottom'].set_color('#4A4A4A'); ax3.spines['left'].set_color('#4A4A4A')

    # ── 4. Claim Amount Distribution by Status ──
    ax4 = fig.add_subplot(gs[1, :2])
    ax4.set_facecolor('#1A252F')
    status_order = ['Approved','Pending','Rejected','Under Investigation','Settled']
    plot_data = [claims[claims['status'] == s]['claimed_amount'].dropna()
                 for s in status_order if s in claims['status'].values]
    labels    = [s for s in status_order if s in claims['status'].values]
    bp = ax4.boxplot(plot_data, labels=labels, patch_artist=True,
                     medianprops=dict(color='white', linewidth=2),
                     whiskerprops=dict(color='#AAB7B8'),
                     capprops=dict(color='#AAB7B8'))
    for patch, color in zip(bp['boxes'], PALETTE):
        patch.set_facecolor(color); patch.set_alpha(0.7)
    ax4.set_title('Claimed Amount Distribution by Claim Status',
                  color='white', fontweight='bold', fontsize=13)
    ax4.set_ylabel('Claimed Amount (₹)', color='white')
    ax4.tick_params(colors='white', axis='both')
    [ax4.spines[s].set_visible(False) for s in ['top', 'right']]
    ax4.spines['bottom'].set_color('#4A4A4A'); ax4.spines['left'].set_color('#4A4A4A')

    # ── 5. Top 10 Cities by Fraud Count ──
    ax5 = fig.add_subplot(gs[1, 2])
    ax5.set_facecolor('#1A252F')
    city_fraud = (claims[claims['fraud_flag']==True]
                  .groupby('city').size()
                  .sort_values(ascending=False)
                  .head(10))
    colors_c = [COLORS['danger'] if v == city_fraud.max() else COLORS['warning']
                if v >= city_fraud.quantile(0.75) else COLORS['info']
                for v in city_fraud.values]
    bars = ax5.barh(city_fraud.index, city_fraud.values, color=colors_c,
                    alpha=0.85, edgecolor='none')
    for bar, val in zip(bars, city_fraud.values):
        ax5.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
                 str(val), va='center', fontsize=9, color='white')
    ax5.set_title('Top 10 Cities by Fraud Claims', color='white', fontweight='bold')
    ax5.set_xlabel('Fraud Claim Count', color='white')
    ax5.tick_params(colors='white')
    [ax5.spines[s].set_visible(False) for s in ['top', 'right']]
    ax5.spines['bottom'].set_color('#4A4A4A'); ax5.spines['left'].set_color('#4A4A4A')

    plt.savefig(f"{OUT_DIR}/dashboard_page2_risk.png",
                dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close()
    print(f"✅ Saved: dashboard_page2_risk.png")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("Loading data...")
    claims, policies, customers, agents, pt = load_data()
    print(f"Loaded {len(claims):,} claims for visualization\n")

    print("Generating Dashboard Page 1: Overview & Fraud Analysis...")
    plot_page1_overview(claims, policies)

    print("Generating Dashboard Page 2: Customer Risk & Agent Performance...")
    plot_page2_risk(claims)

    print(f"\n✅ Dashboard complete! Charts saved to '{OUT_DIR}/'")

if __name__ == "__main__":
    main()

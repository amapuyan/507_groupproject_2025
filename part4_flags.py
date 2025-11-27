# **Contributors:**  
# Amy Kim & Jonathan Jafari collaborated on the development and refinement of the flagging logic implemented in `part4_flags.py`.
# Chenkun Xiang contributed to the optimization of data processing and query efficiency.

# Research Question: How does mRSI behave as a marker of neuromuscular fatigue compared to output metrics 
# like Jump Height and Propulsive Net Impulse over the competitive season in NCAA athletes? 

# Article referenced for mRSI average range (0.208 to 0.704): 
# https://www.researchgate.net/publication/328590949_Preliminary_Scale_of_Reference_Values_for_Evaluating_Reactive_Strength_Index-Modified_in_Male_and_Female_NCAA_Division_I_Athletes
# Research shows 0.63 = 97th percentile in NCAA Division I athletes, 0.42 = 50th percentile, 0.21 = 3rd percentile

# Typical Error (TE): Studies have reported typical error values for RSImod to be between 7.5% and 9.3%
# https://www.researchgate.net/publication/268804309_Using_Reactive_Strength_Index-Modified_as_an_Explosive_Performance_Measurement_Tool_in_Division_I_Athletes

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE")

connection_string = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(connection_string)

# Load only the metrics we need with optimized query
query = text(f"""
    SELECT playername, team, timestamp, metric, value
    FROM {DB_TABLE}
    WHERE metric IN ('mRSI', 'Jump Height(m)', 'Propulsive Net Impulse(N.s)')
        AND value IS NOT NULL
    LIMIT 50000
""")
df = pd.read_sql(query, engine, parse_dates=['timestamp'])

engine.dispose()

print("\nCleaning data...")
# Remove any null values
df = df.dropna(subset=['value', 'timestamp'])

# Ensure 'value' is numeric
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df = df.dropna(subset=['value'])  # Remove any that failed conversion

# Pre-create boolean masks for better performance
is_mrsi = df['metric'] == 'mRSI'
is_jh = df['metric'] == 'Jump Height(m)'
is_pni = df['metric'] == 'Propulsive Net Impulse(N.s)'

print(f"Loaded {len(df)} records for {df['playername'].nunique()} athletes")

# ==============================
# Calculate baselines for all metrics
# ==============================
print("Calculating baselines...")

# OPTIMIZED: Single pivot operation instead of 3 separate groupby operations
baseline_metrics = df.pivot_table(
    index='playername',
    columns='metric',
    values='value',
    aggfunc='median'
)

# Safely rename columns - only rename columns that actually exist
rename_mapping = {
    'mRSI': 'baseline_mRSI',
    'Jump Height(m)': 'baseline_jh',
    'Propulsive Net Impulse(N.s)': 'baseline_pni'
}
# Only rename columns that exist in the dataframe
existing_renames = {k: v for k, v in rename_mapping.items() if k in baseline_metrics.columns}
baseline_metrics = baseline_metrics.rename(columns=existing_renames)

# OPTIMIZED: Single merge operation instead of 3 separate merges
df = df.merge(baseline_metrics, left_on='playername', right_index=True, how='left')

# Check which baseline columns actually exist
print(f"Available baseline columns: {[col for col in df.columns if 'baseline_' in col]}")

# Threshold 2: Deviation from team average mRSI by more than 15%
# Calculate team average mRSI
if is_mrsi.sum() > 0:
    team_avg_mRSI = df[is_mrsi]['value'].mean()
    print(f"Team average mRSI: {team_avg_mRSI:.3f}")
else:
    team_avg_mRSI = 0
    print("Warning: No mRSI data available")

print("Applying flag thresholds...")

# Threshold 1: flag mRSI if it drops 10% below their baseline indicating that they are getting fatigued
if 'baseline_mRSI' in df.columns:
    df['mRSI_flag'] = np.where(
        is_mrsi & (df['value'] < 0.9 * df['baseline_mRSI']),
        1,
        0
    )
else:
    df['mRSI_flag'] = 0
    print("Warning: No mRSI baseline data available")

# Threshold 2: Deviation from team average mRSI by more than 15%
# Flag mRSI values that deviate more than 15% from team average
if is_mrsi.sum() > 0:
    df['mRSI_team_flag'] = np.where(
        is_mrsi & ((df['value'] < 0.85 * team_avg_mRSI) | (df['value'] > 1.15 * team_avg_mRSI)),
        1,
        0
    )
else:
    df['mRSI_team_flag'] = 0
    print("Warning: No mRSI data available for team average")

# Threshold 3: Jump Height drop ≥7% vs player baseline
if 'baseline_jh' in df.columns:
    df['jh_flag'] = np.where(
        is_jh & (df['value'] < 0.93 * df['baseline_jh']), 
        1,
        0
    )
else:
    df['jh_flag'] = 0
    print("Warning: No Jump Height baseline data available")

# Threshold 4: Propulsive Net Impulse drop ≥7% vs player baseline
if 'baseline_pni' in df.columns:
    df['pni_flag'] = np.where(
        is_pni & (df['value'] < 0.93 * df['baseline_pni']),
        1,
        0
    )
else:
    df['pni_flag'] = 0
    print("Warning: No Propulsive Net Impulse baseline data available")

# ==============================
# Build flagged athletes CSV
# ==============================
print("Generating flagged athletes report...")

flag_cols = ['mRSI_flag', 'mRSI_team_flag', 'jh_flag', 'pni_flag']

def build_flag_reason(row):
    reasons = []
    if row.get('mRSI_flag', 0) == 1:
        reasons.append("mRSI drop ≥10% vs baseline")
    if row.get('mRSI_team_flag', 0) == 1:
        reasons.append("mRSI >15% from team average")
    if row.get('jh_flag', 0) == 1:
        reasons.append("Jump Height(m) drop ≥7% vs baseline")
    if row.get('pni_flag', 0) == 1:
        reasons.append("Propulsive Net Impulse(N.s) drop ≥7% vs baseline")
    return "; ".join(reasons)

# Keep only rows where at least one flag was triggered
flagged = df[df[flag_cols].sum(axis=1) > 0].copy()

if len(flagged) > 0:
    # Create human-readable flag_reason text
    flagged['flag_reason'] = flagged.apply(build_flag_reason, axis=1)
    
    # Prepare required columns
    flagged_out = flagged.assign(
        metric_value=flagged['value'],
        last_test_date=flagged['timestamp'].dt.date.astype(str)
    )[['playername', 'team', 'flag_reason', 'metric_value', 'last_test_date']].drop_duplicates()
    
    # Sort for easier review
    flagged_out = flagged_out.sort_values(['playername', 'last_test_date'])
    
    # Save CSV
    flagged_out.to_csv("part4_flagged_athletes.csv", index=False)
    
    print(f"\n✓ Saved {len(flagged_out)} flagged records to part4_flagged_athletes.csv")
    print(f"✓ {flagged_out['playername'].nunique()} unique athletes flagged")
    
    # Summary statistics
    print("\nFlag breakdown:")
    print(f"  - mRSI baseline drop: {flagged['mRSI_flag'].sum()} instances")
    print(f"  - mRSI team deviation: {flagged['mRSI_team_flag'].sum()} instances")
    print(f"  - Jump Height drop: {flagged['jh_flag'].sum()} instances")
    print(f"  - Propulsive Net Impulse drop: {flagged['pni_flag'].sum()} instances")
else:
    print("\n✓ No athletes currently meet flag criteria")

print("\nAnalysis complete!")
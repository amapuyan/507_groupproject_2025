# **Contributors:**  
# Amy Kim & Jonathan Jafari collaborated on the development and refinement of the flagging logic implemented in `part4_flags.py`.
# Chenkun Xiang contributed to the optimization of data processing and query efficiency.
# Xiao Hong Chen contributed with making tables summarizing the flagging results, used in part4_research_synthesis.pdf.

# ==============================
# Research Question: 
# How does mRSI behave as a marker of neuromuscular fatigue compared to output metrics 
# like Jump Height and Propulsive Net Impulse over the competitive season in NCAA athletes?
# ==============================

# Scientific Background:
# Article referenced for mRSI average range (0.208 to 0.704): 
# https://www.researchgate.net/publication/328590949_Preliminary_Scale_of_Reference_Values_for_Evaluating_Reactive_Strength_Index-Modified_in_Male_and_Female_NCAA_Division_I_Athletes
# Research shows 0.63 = 97th percentile in NCAA Division I athletes, 0.42 = 50th percentile, 0.21 = 3rd percentile

# Typical Error (TE): Studies have reported typical error values for RSImod to be between 7.5% and 9.3%
# https://www.researchgate.net/publication/268804309_Using_Reactive_Strength_Index-Modified_as_an_Explosive_Performance_Measurement_Tool_in_Division_I_Athletes

# ==============================
# Flagging Logic Overview:
# ==============================
# This script implements four fatigue detection thresholds:
# 
# 1. mRSI Baseline Drop (Threshold 1): Flags when an athlete's mRSI drops ≥10% below their personal baseline
#    - Rationale: Indicates individual neuromuscular fatigue relative to their established performance
#
# 2. mRSI Team Deviation (Threshold 2): Flags when an athlete's mRSI deviates >15% from team average
#    - Rationale: Identifies outliers who may be under-recovered or over-performing relative to peers
#
# 3. Jump Height Baseline Drop (Threshold 3): Flags when Jump Height drops ≥7% below personal baseline
#    - Rationale: Jump height is a reliable output metric; 7% drop exceeds typical error and suggests fatigue
#
# 4. Propulsive Net Impulse Baseline Drop (Threshold 4): Flags when PNI drops ≥7% below personal baseline
#    - Rationale: PNI reflects explosive power output; 7% drop indicates compromised force production
#
# Output: CSV file with flagged athletes, reasons for flagging, and test dates for coaching review

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np

load_dotenv()

# ==============================
# Database Connection Constants
# ==============================
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE")

# ==============================
# Threshold Constants (based on research literature)
# ==============================
# Threshold 1: mRSI baseline drop - 10% decline indicates fatigue
MRSI_BASELINE_DROP_THRESHOLD = 0.10

# Threshold 2: mRSI team deviation - 15% from average flags outliers
MRSI_TEAM_DEVIATION_THRESHOLD = 0.15

# Threshold 3: Jump Height baseline drop - 7% exceeds typical error (7.5-9.3%)
JH_BASELINE_DROP_THRESHOLD = 0.07

# Threshold 4: Propulsive Net Impulse baseline drop - 7% indicates compromised power output
PNI_BASELINE_DROP_THRESHOLD = 0.07

# ==============================
# Metric Name Constants
# ==============================
METRIC_MRSI = 'mRSI'
METRIC_JH = 'Jump Height(m)'
METRIC_PNI = 'Propulsive Net Impulse(N.s)'

# ==============================
# Baseline Column Name Constants
# ==============================
COL_BASELINE_MRSI = 'baseline_mRSI'
COL_BASELINE_JH = 'baseline_jh'
COL_BASELINE_PNI = 'baseline_pni'

# ==============================
# Flag Column Name Constants
# ==============================
COL_MRSI_FLAG = 'mRSI_flag'
COL_MRSI_TEAM_FLAG = 'mRSI_team_flag'
COL_JH_FLAG = 'jh_flag'
COL_PNI_FLAG = 'pni_flag'

# ==============================
# Database Connection & Data Loading
# ==============================
connection_string = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(connection_string)

# Load only the metrics we need with optimized query
query = text(f"""
    SELECT playername, team, timestamp, metric, value
    FROM {DB_TABLE}
    WHERE metric IN ('{METRIC_MRSI}', '{METRIC_JH}', '{METRIC_PNI}')
        AND value IS NOT NULL
    LIMIT 50000
""")
df = pd.read_sql(query, engine, parse_dates=['timestamp'])

engine.dispose()

# ==============================
# Data Cleaning
# ==============================
print("\nCleaning data...")
# Remove any null values
df = df.dropna(subset=['value', 'timestamp'])

# Ensure 'value' is numeric
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df = df.dropna(subset=['value'])  # Remove any that failed conversion

# Pre-create boolean masks for better performance
is_mrsi = df['metric'] == METRIC_MRSI
is_jh = df['metric'] == METRIC_JH
is_pni = df['metric'] == METRIC_PNI

print(f"Loaded {len(df)} records for {df['playername'].nunique()} athletes")

# Check which metrics actually exist in the data
available_metrics = df['metric'].unique()
print(f"Available metrics in data: {list(available_metrics)}")

# ==============================
# Calculate Baselines for All Metrics
# ==============================
# Baseline = median value for each athlete across all their tests
# Rationale: Median is robust to outliers and represents typical performance level
print("\nCalculating baselines...")

# OPTIMIZED: Single pivot operation instead of 3 separate groupby operations
baseline_metrics = df.pivot_table(
    index='playername',
    columns='metric',
    values='value',
    aggfunc='median'  # Using median as baseline (robust to outliers)
)

# Dynamically rename only the columns that exist
rename_mapping = {
    METRIC_MRSI: COL_BASELINE_MRSI,
    METRIC_JH: COL_BASELINE_JH,
    METRIC_PNI: COL_BASELINE_PNI
}
# Only rename columns that exist in the dataframe (handles missing metrics gracefully)
existing_renames = {k: v for k, v in rename_mapping.items() if k in baseline_metrics.columns}
baseline_metrics = baseline_metrics.rename(columns=existing_renames)

# OPTIMIZED: Single merge operation instead of 3 separate merges
df = df.merge(baseline_metrics, left_on='playername', right_index=True, how='left')

# Check which baseline columns actually exist
available_baselines = [col for col in df.columns if 'baseline_' in col]
print(f"Available baseline columns: {available_baselines}")

# ==============================
# Threshold 2 Setup: Team Average mRSI
# ==============================
# Calculate team average mRSI (only if mRSI data exists)
if is_mrsi.sum() > 0:
    team_avg_mRSI = df[is_mrsi]['value'].mean()
    print(f"Team average mRSI: {team_avg_mRSI:.3f}")
else:
    team_avg_mRSI = None
    print("Warning: No mRSI data available")

print("\nApplying flag thresholds...")

# ==============================
# Apply Flag Thresholds
# ==============================

# Threshold 1: flag mRSI if it drops 10% below their baseline indicating that they are getting fatigued
if COL_BASELINE_MRSI in df.columns and is_mrsi.sum() > 0:
    df[COL_MRSI_FLAG] = np.where(
        is_mrsi & (df['value'] < (1 - MRSI_BASELINE_DROP_THRESHOLD) * df[COL_BASELINE_MRSI]),
        1,
        0
    )
    print(f"✓ Applied mRSI baseline drop threshold ({MRSI_BASELINE_DROP_THRESHOLD*100:.0f}%)")
else:
    df[COL_MRSI_FLAG] = 0
    print("⚠ Skipped mRSI baseline drop threshold (no data)")

# Threshold 2: Deviation from team average mRSI by more than 15%
# Flag mRSI values that deviate more than 15% from team average
if is_mrsi.sum() > 0 and team_avg_mRSI is not None:
    df[COL_MRSI_TEAM_FLAG] = np.where(
        is_mrsi & (
            (df['value'] < (1 - MRSI_TEAM_DEVIATION_THRESHOLD) * team_avg_mRSI) | 
            (df['value'] > (1 + MRSI_TEAM_DEVIATION_THRESHOLD) * team_avg_mRSI)
        ),
        1,
        0
    )
    print(f"✓ Applied mRSI team deviation threshold ({MRSI_TEAM_DEVIATION_THRESHOLD*100:.0f}%)")
else:
    df[COL_MRSI_TEAM_FLAG] = 0
    print("⚠ Skipped mRSI team deviation threshold (no data)")

# Threshold 3: Jump Height drop ≥7% vs player baseline
if COL_BASELINE_JH in df.columns and is_jh.sum() > 0:
    df[COL_JH_FLAG] = np.where(
        is_jh & (df['value'] < (1 - JH_BASELINE_DROP_THRESHOLD) * df[COL_BASELINE_JH]), 
        1,
        0
    )
    print(f"✓ Applied Jump Height baseline drop threshold ({JH_BASELINE_DROP_THRESHOLD*100:.0f}%)")
else:
    df[COL_JH_FLAG] = 0
    print("⚠ Skipped Jump Height baseline drop threshold (no data)")

# Threshold 4: Propulsive Net Impulse drop ≥7% vs player baseline
if COL_BASELINE_PNI in df.columns and is_pni.sum() > 0:
    df[COL_PNI_FLAG] = np.where(
        is_pni & (df['value'] < (1 - PNI_BASELINE_DROP_THRESHOLD) * df[COL_BASELINE_PNI]),
        1,
        0
    )
    print(f"✓ Applied Propulsive Net Impulse baseline drop threshold ({PNI_BASELINE_DROP_THRESHOLD*100:.0f}%)")
else:
    df[COL_PNI_FLAG] = 0
    print("⚠ Skipped Propulsive Net Impulse baseline drop threshold (no data)")

# ==============================
# Build Flagged Athletes CSV
# ==============================
print("\nGenerating flagged athletes report...")

flag_cols = [COL_MRSI_FLAG, COL_MRSI_TEAM_FLAG, COL_JH_FLAG, COL_PNI_FLAG]

def build_flag_reason(row):
    """
    Build human-readable flag reason text for each flagged athlete record.
    Returns a semicolon-separated string of all flag reasons that were triggered.
    """
    reasons = []
    if row.get(COL_MRSI_FLAG, 0) == 1:
        reasons.append(f"mRSI drop ≥{MRSI_BASELINE_DROP_THRESHOLD*100:.0f}% vs baseline")
    if row.get(COL_MRSI_TEAM_FLAG, 0) == 1:
        reasons.append(f"mRSI >{MRSI_TEAM_DEVIATION_THRESHOLD*100:.0f}% from team average")
    if row.get(COL_JH_FLAG, 0) == 1:
        reasons.append(f"Jump Height(m) drop ≥{JH_BASELINE_DROP_THRESHOLD*100:.0f}% vs baseline")
    if row.get(COL_PNI_FLAG, 0) == 1:
        reasons.append(f"Propulsive Net Impulse(N.s) drop ≥{PNI_BASELINE_DROP_THRESHOLD*100:.0f}% vs baseline")
    return "; ".join(reasons)

# Keep only rows where at least one flag was triggered
flagged = df[df[flag_cols].sum(axis=1) > 0].copy()

if len(flagged) > 0:
    # Create human-readable flag_reason text
    flagged['flag_reason'] = flagged.apply(build_flag_reason, axis=1)
    
    # Prepare required columns for output
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
    print(f"  - mRSI baseline drop: {flagged[COL_MRSI_FLAG].sum()} instances")
    print(f"  - mRSI team deviation: {flagged[COL_MRSI_TEAM_FLAG].sum()} instances")
    print(f"  - Jump Height drop: {flagged[COL_JH_FLAG].sum()} instances")
    print(f"  - Propulsive Net Impulse drop: {flagged[COL_PNI_FLAG].sum()} instances")
else:
    print("\n✓ No athletes currently meet flag criteria")

# ==============================
# Generate Summary Tables
# ==============================
print("\nGenerating summary tables...")

table1 = pd.DataFrame({
    "Characteristic": [
        "Total CMJ records analyzed",
        "Unique athletes",
        "Unique teams",
        "Available CMJ metrics",
        "Baseline method",
        "Study design",
        "Monitoring period"
    ],
    "Value": [
        len(df),
        df['playername'].nunique(),
        df['team'].nunique(),
        ", ".join(sorted(available_metrics)),
        "Median of all available tests per athlete",
        "Longitudinal, observational",
        "Competitive seasons (2018–2025)"
    ]
})

print("\nDataset Summary (Table 1):")
print(table1)


table2 = pd.DataFrame({
        "Flag Type": [
            "mRSI baseline drop (≥10% vs baseline)",
            "mRSI team deviation (>15% from team average)",
            "Jump Height baseline drop (≥7% vs baseline)",
            "Propulsive Net Impulse drop (≥7% vs baseline)",
            "Total flagged tests",
            "Unique athletes flagged"
        ],
        "Number of Flagged Instances": [
            int(flagged[COL_MRSI_FLAG].sum()),
            int(flagged[COL_MRSI_TEAM_FLAG].sum()),
            int(flagged[COL_JH_FLAG].sum()),
            int(flagged[COL_PNI_FLAG].sum()),
            int(len(flagged_out)),
            int(flagged_out["playername"].nunique())
        ]
    })

print("\nFlagging Summary (Table 2):")
print(table2)






print("\nAnalysis complete!")
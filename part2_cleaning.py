# ----------------------------------------------
# Part 2 – Data Cleaning & Missing Data Overview
# Lead Contributor: Jonathan Jafari
# Description: Load main table, convert dtypes,
#              and generate missing-data summaries
#              plus coverage statistics.
# ----------------------------------------------

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# 1. Load environment variables (same as Part 1)
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE", "research_experiment_refactor_test")

# 2. Build connection string and create engine
connection_string = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

print("Connecting using:")
print(connection_string)

engine = create_engine(connection_string)

# -------------------------------------------------
# 3. Load data from the main table
#    For now we load ALL rows.
#    If this ever becomes too slow, we can add a date filter.
# -------------------------------------------------

query = f"""
SELECT *
FROM {DB_TABLE}
"""

print("\nLoading data into pandas DataFrame...")
df = pd.read_sql(text(query), engine)
print("Done!\n")

print("Basic shape of the raw data:")
print(df.shape)  # (rows, columns)

# -------------------------------------------------
# 4. Basic cleaning / type conversions
# -------------------------------------------------

# Convert timestamp to datetime (if not already)
print("\nConverting 'timestamp' to datetime (if needed)...")
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# Ensure 'value' is numeric
print("Ensuring 'value' column is numeric...")
df["value"] = pd.to_numeric(df["value"], errors="coerce")

print("\nData types after conversion:")
print(df.dtypes)

# -------------------------------------------------
# 5. Missing data overview (Part 2.1 starter)
# -------------------------------------------------

print("\n--- Missing Values Per Column ---")
missing_counts = df.isna().sum()
print(missing_counts)

print("\n--- Percentage of Missing Values Per Column ---")
missing_percent = (df.isna().sum() / len(df)) * 100
print(missing_percent.round(2))

# OPTIONAL: save this summary to CSV so we can reference it
missing_summary = pd.DataFrame(
    {
        "missing_count": missing_counts,
        "missing_percent": missing_percent,
    }
).sort_values("missing_percent", ascending=False)

missing_summary.to_csv("part2_missing_summary_overall.csv")
print("\nSaved overall missing data summary to 'part2_missing_summary_overall.csv'")

# -------------------------------------------------
# 6. Focus on selected metrics (update with our choices later) accel_load_max and distance_total
# -------------------------------------------------

SELECTED_METRICS = [
    "Jump Height(m)",
    "Peak Propulsive Force(N)",
    "Peak Velocity(m/s)",
    "Propulsive Net Impulse(N.s)",
    "mRSI",
]

df_selected = df[df["metric"].isin(SELECTED_METRICS)].copy()

print("\nFiltered to selected metrics:")
print(df_selected["metric"].value_counts())
print("Shape of df_selected:", df_selected.shape)

# -------------------------------------------------
# 7. Missing or zero values per selected metric
# -------------------------------------------------

df_selected["is_missing_or_zero"] = df_selected["value"].isna() | (df_selected["value"] == 0)

metric_missing = (
    df_selected
    .groupby("metric")["is_missing_or_zero"]
    .mean()
    .reset_index()
    .rename(columns={"is_missing_or_zero": "missing_or_zero_percent"})
)

metric_missing["missing_or_zero_percent"] = metric_missing["missing_or_zero_percent"] * 100

print("\n--- Missing or Zero Values by Selected Metric (%) ---")
print(metric_missing)

metric_missing.to_csv("part2_selected_metrics_missing_zero.csv", index=False)
print("\nSaved selected metric missing summary to 'part2_selected_metrics_missing_zero.csv'")

# -------------------------------------------------
# 8. Athletes with ≥5 measurements per metric
# -------------------------------------------------

measurement_counts = (
    df_selected
    .groupby(["team", "playername", "metric"])
    .size()
    .reset_index(name="num_measurements")
)

measurement_counts["has_5_or_more"] = measurement_counts["num_measurements"] >= 5

team_coverage = (
    measurement_counts
    .groupby("team")
    .agg(
        total_athletes=("playername", "nunique"),
        athletes_with_5plus=("has_5_or_more", "sum"),
    )
)

team_coverage["percent_athletes_with_5plus"] = (
    team_coverage["athletes_with_5plus"] / team_coverage["total_athletes"] * 100
)

print("\n--- Team Coverage (athletes with ≥5 measurements) ---")
print(team_coverage)

team_coverage.to_csv("part2_team_coverage_5plus.csv")
print("\nSaved team coverage summary to 'part2_team_coverage_5plus.csv'")

# -------------------------------------------------
# 9. Athletes not tested in last 6 months
# -------------------------------------------------

latest_date = df_selected["timestamp"].max()
cutoff_date = latest_date - pd.DateOffset(months=6)

last_test = (
    df_selected
    .groupby("playername")["timestamp"]
    .max()
    .reset_index()
    .rename(columns={"timestamp": "last_test_date"})
)

inactive_athletes = last_test[last_test["last_test_date"] < cutoff_date]

print("\nLatest test date in data:", latest_date)
print("Cutoff date (6 months before):", cutoff_date)

print("\n--- Athletes NOT tested in last 6 months (selected metrics) ---")
print(inactive_athletes.head())

inactive_athletes.to_csv("part2_inactive_athletes_6months.csv", index=False)
print("\nSaved inactive athletes list to 'part2_inactive_athletes_6months.csv'")

# -------------------------------------------------
# 10. Data Transformation: Long to Wide Format
# Contributor: Anthony Mapuyan
# -------------------------------------------------
# -------------------------------------------------
# 10. Data Transformation: Long → Wide for a Single Player (Part 2.2)
# -------------------------------------------------

def player_long_to_wide(df_source, player_name, metrics, fill_method=None):
    """
    Transform long-format data for a single player into wide format.
    """

    # 1) Filter to the selected player and metrics
    player_df = df_source[
        (df_source["playername"] == player_name) &
        (df_source["metric"].isin(metrics))
    ].copy()

    if player_df.empty:
        print(f"[WARNING] No rows found for player {player_name} with given metrics.")
        return pd.DataFrame(columns=["timestamp"] + metrics)

    # 2) Convert timestamp and sort
    player_df["timestamp"] = pd.to_datetime(player_df["timestamp"], errors="coerce")
    player_df = player_df.sort_values(["timestamp", "metric"])

    # 3) Pivot long → wide
    wide_df = (
        player_df
        .pivot_table(
            index="timestamp",
            columns="metric",
            values="value",
            aggfunc="mean"
        )
        .reset_index()
        .sort_values("timestamp")
    )

    # 4) Missing value handling
    if fill_method == "ffill":
        wide_df = wide_df.fillna(method="ffill")
    elif fill_method == "bfill":
        wide_df = wide_df.fillna(method="bfill")
    elif fill_method == "zero":
        wide_df = wide_df.fillna(0)

    # 5) Order columns
    ordered_cols = ["timestamp"] + [m for m in metrics if m in wide_df.columns]
    wide_df = wide_df[ordered_cols]

    return wide_df

print("\n--- Testing Part 2.2 on 3 athletes ---")

example_players = df_selected["playername"].unique()[:3]

for p in example_players:
    print(f"\n===== {p} =====")
    wide = player_long_to_wide(df_selected, p, SELECTED_METRICS)
    print(wide.head())

# -------------------------------------------------
# Creating a Derived Metric (Part 2.3)
# Contributor: Amy Kim
# -------------------------------------------------

# 1. Calculates the mean value for each team (using the team column)
team_metric_average = (
    df_selected
    .groupby(["team", "metric"])[["value"]]
    .mean()
    .reset_index()
    .rename(columns={"value": "average_value", "team": "team"})
)
print("\n--- Team Average Values for Selected Metrics ---")
print(team_metric_average)

# 2. For each athlete measurement, calculates their percent difference from their team's average
df_selected = df_selected.merge(
    team_metric_average,
    on=["team", "metric"],
    how="left"
)
df_selected["percent_difference_from_team_avg"] = (
    (df_selected["value"] - df_selected["average_value"]) / df_selected["average_value"]
) * 100 
print("\n--- Sample Athlete Measurements with Percent Difference from Team Average ---")
print(df_selected[["team", "playername", "metric", "value", "average_value", "percent_difference_from_team_avg"]])

# 3. Identify the top 5 unique and bottom 5 unique performers relative to their team mean
top_performers = (
    df_selected
    .sort_values("percent_difference_from_team_avg", ascending=False)
    .drop_duplicates(subset=["playername"])
    .head(5)
    [["playername", "team", "metric", "percent_difference_from_team_avg"]]
)

bottom_performers = (
    df_selected
    .sort_values("percent_difference_from_team_avg", ascending=True)
    .drop_duplicates(subset=["playername"])
    .head(5)
    [["playername", "team", "metric", "percent_difference_from_team_avg"]]
)
print("\n--- Top 5 Unique Performers Relative to Team Average ---")
print(top_performers)       
print("\n--- Bottom 5 Unique Performers Relative to Team Average ---")
print(bottom_performers)

# -------------------------------------------------
# 11. Close the connection
# -------------------------------------------------
engine.dispose()
print("\nConnection closed.")

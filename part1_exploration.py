# ----------------------------------------------
# Part 1 – Data Exploration
# Contributor: Jonathan Jafari
# Description: Connection test and high-level
#              data quality / structure overview.
# ----------------------------------------------

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Load variables from .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE", "research_experiment_refactor_test")

# Build connection string
connection_string = (
    f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

print("Connecting using:")
print(connection_string)

# Create engine
engine = create_engine(connection_string)

# -------------------------
#  FIRST 10 ROWS
# -------------------------
print("\nRunning test query...\n")
query = f"SELECT * FROM {DB_TABLE} LIMIT 10;"

try:
    df = pd.read_sql(text(query), engine)
    print("SUCCESS! First 10 rows:\n")
    print(df)
except Exception as e:
    print("ERROR OCCURRED:\n")
    print(e)

# -------------------------
# 1. Unique Athletes
# -------------------------
print("\n--- 1. Unique Athletes ---")
query_unique_athletes = f"""
SELECT COUNT(DISTINCT playername) AS unique_athletes
FROM {DB_TABLE};
"""
print(pd.read_sql(text(query_unique_athletes), engine))

# -------------------------
# 2. Number of Teams
# -------------------------
print("\n--- 2. Number of Teams/Sports ---")
query_teams = f"""
SELECT COUNT(DISTINCT team) AS num_teams
FROM {DB_TABLE};
"""
print(pd.read_sql(text(query_teams), engine))

# -------------------------
# 3. Date Range
# -------------------------
print("\n--- 3. Date Range ---")
query_dates = f"""
SELECT MIN(timestamp) AS earliest,
       MAX(timestamp) AS latest
FROM {DB_TABLE};
"""
print(pd.read_sql(text(query_dates), engine))

# -------------------------
# 4. Records Per Source
# -------------------------
print("\n--- 4. Records Per Data Source ---")
query_sources = f"""
SELECT data_source,
       COUNT(*) AS record_count
FROM {DB_TABLE}
GROUP BY data_source
ORDER BY record_count DESC;
"""
print(pd.read_sql(text(query_sources), engine))

# -------------------------
# 5. Missing Names
# -------------------------
print("\n--- 5. Missing or Invalid Player Names ---")
query_missing_names = f"""
SELECT COUNT(*) AS missing_or_invalid_names
FROM {DB_TABLE}
WHERE playername IS NULL
   OR playername = ''
   OR playername LIKE 'NULL';
"""
print(pd.read_sql(text(query_missing_names), engine))

# -------------------------
# 6. Athletes with >= 2 Data Sources
# -------------------------
print("\n--- 6. Athletes With Data From 2 or More Sources ---")
query_multisource = f"""
SELECT playername,
       COUNT(DISTINCT data_source) AS num_sources
FROM {DB_TABLE}
GROUP BY playername
HAVING num_sources >= 2
ORDER BY num_sources DESC;
"""
print(pd.read_sql(text(query_multisource), engine))

# -------------------------
# 7. Unique Metrics
# -------------------------
print("\n--- 7. Total Number of Unique Metrics ---")
query_unique_metrics = f"""
SELECT COUNT(DISTINCT metric) AS num_unique_metrics
FROM {DB_TABLE};
"""
print(pd.read_sql(text(query_unique_metrics), engine))

# -------------------------
# 8. Hawkins Top 10 Metrics
# -------------------------
print("\n--- 8. Top 10 Metrics for HAWKINS ---")
query_hawkins_metrics = f"""
SELECT
    metric,
    COUNT(*) AS record_count,
    COUNT(DISTINCT playername) AS num_athletes,
    MIN(timestamp) AS earliest_date,
    MAX(timestamp) AS latest_date
FROM {DB_TABLE}
WHERE data_source = 'hawkins'
GROUP BY metric
ORDER BY record_count DESC
LIMIT 10;
"""
print(pd.read_sql(text(query_hawkins_metrics), engine))

# -------------------------
# 9. Kinexon Top 10 Metrics
# -------------------------
print("\n--- 9. Top 10 Metrics for KINEXON ---")
query_kinexon_metrics = f"""
SELECT
    metric,
    COUNT(*) AS record_count,
    COUNT(DISTINCT playername) AS num_athletes,
    MIN(timestamp) AS earliest_date,
    MAX(timestamp) AS latest_date
FROM {DB_TABLE}
WHERE data_source = 'kinexon'
GROUP BY metric
ORDER BY record_count DESC
LIMIT 10;
"""
print(pd.read_sql(text(query_kinexon_metrics), engine))

# -------------------------
# 10. Vald Top 10 Metrics
# -------------------------
print("\n--- 10. Top 10 Metrics for VALD ---")
query_vald_metrics = f"""
SELECT
    metric,
    COUNT(*) AS record_count,
    COUNT(DISTINCT playername) AS num_athletes,
    MIN(timestamp) AS earliest_date,
    MAX(timestamp) AS latest_date
FROM {DB_TABLE}
WHERE data_source = 'vald'
GROUP BY metric
ORDER BY record_count DESC
LIMIT 10;
"""
print(pd.read_sql(text(query_vald_metrics), engine))

# -------------------------
# 11. Summary Per Source
# -------------------------
print("\n--- 11. Date Range & Record Count by Data Source ---")
query_source_summary = f"""
SELECT
    data_source,
    COUNT(*) AS record_count,
    MIN(timestamp) AS earliest_date,
    MAX(timestamp) AS latest_date
FROM {DB_TABLE}
GROUP BY data_source
ORDER BY record_count DESC;
"""
print(pd.read_sql(text(query_source_summary), engine))

# -------------------------
# Close connection
# -------------------------
engine.dispose()


# ----------------------------------------------
# Part 1.4 – Metric Summary to Assist With Choosing Metrics
# Contributor: Xiao Hong Chen
# Description: Summary statistics for each metric to find suitable metrics for analysis
# ----------------------------------------------

metricquery = f"""
SELECT team, playername, metric 
FROM {DB_TABLE}
"""

df = pd.read_sql(text(metricquery), engine)

# 1. Total number of rows per metric
metric_counts = df_all.groupby("metric").size().rename("row_count")

# 2. Number of unique teams per metric
team_counts = df_all.groupby("metric")["team"].nunique().rename("num_teams")

# 3. Number of unique athletes per metric
athlete_counts = df_all.groupby("metric")["playername"].nunique().rename("num_athletes")

# Combine into one DataFrame
metric_summary = pd.concat([metric_counts, team_counts, athlete_counts], axis=1)

# Sort by row_count (most common metrics at top)
metric_summary = metric_summary.sort_values("row_count", ascending=False)

metric_summary.head(20)
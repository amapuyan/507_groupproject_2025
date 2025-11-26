# Research Question: How does mRSI behave as a marker of neuromuscular fatigue compared to output metrics like Jump Height and Propulsive Net Impulse over the competitive season in NCAA athletes? 

# Article referenced for mRSI average range (0.208 to 0.704): https://www.researchgate.net/publication/328590949_Preliminary_Scale_of_Reference_Values_for_Evaluating_Reactive_Strength_Index-Modified_in_Male_and_Female_NCAA_Division_I_Athletes
#       Research shows 0.63 = 97th percentile in NCAA Division I athletes, 0.42 = 50th percentile, 0.21 = 3rd percentile

# Typical Error (TE): Studies have reported typical error values for RSImod to be between 7.5% and 9.3%, indicating that a change larger than this range is more likely to represent a genuine physiological change rather than normal test-retest variability.
#https://www.researchgate.net/publication/268804309_Using_Reactive_Strength_Index-Modified_as_an_Explosive_Performance_Measurement_Tool_in_Division_I_Athletes#:~:text=Finally%2C%20independent%20samples%20t%2Dtests,loaded%20countermovement%20jump%20conditions%2C%20respectively. 

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd


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

df = pd.read_sql(text(f"SELECT * FROM {DB_TABLE} LIMIT 50000"), engine)
df.head()


# Convert timestamp to datetime (if not already)
print("\nConverting 'timestamp' to datetime (if needed)...")
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# Ensure 'value' is numeric
print("Ensuring 'value' column is numeric...")
df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Threshold 1: flag mRSI if it drops 10% below their baseline indicating that they are getting fatigued
# Calculate baseline mRSI for each athlete
baseline_mRSI = df[df['metric'] == 'mRSI'].groupby('playername')['value'].mean().reset_index()
baseline_mRSI.columns = ['playername', 'baseline_mRSI']

# Merge baseline mRSI back into the main dataframe
df = df.merge(baseline_mRSI, on='playername', how='left')
# Flag mRSI values that are 10% below baseline
df['mRSI_flag'] = ((df['metric'] == 'mRSI') & (df['value'] < 0.9 * df['baseline_mRSI'])).astype(int)

# Threshold 2: Deviation from team average mRSI by more than 15%
# Calculate team average mRSI
team_avg_mRSI = df[df['metric'] == 'mRSI']['value'].mean()
# Flag mRSI values that deviate more than 15% from team average
df['mRSI_team_flag'] = ((df['metric'] == 'mRSI') & 
                        ((df['value'] < 0.85 * team_avg_mRSI) | (df['value'] > 1.15 * team_avg_mRSI))).astype(int)
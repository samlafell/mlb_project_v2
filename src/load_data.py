from pybaseball.lahman import teams_core
import pandas as pd
import polars as pl
import os
#from prefect import task, flow
import pathlib
from secrets import randbelow

# project_root = pathlib.Path(__file__).resolve().parents[1] # go up one level from current file, command line execution

# Define data Location and Base path
root = pathlib.Path(__file__).parent.absolute()
TABLE_PATH = os.path.join(root, "data", "raw")

# Import utils
from utils.team_performance import schedule_and_record

YEAR = 2023
query_year = YEAR - 1

def get_team_schedule_or_fallback(team, year, team_df):
    try:
        return schedule_and_record(year, team)
    except Exception as e1:
        print(e1)
        try:
            new_team_id = team_df.filter(pl.col("teamID") == team).select("franchID")[
                0, 0
            ]  # Polars dataframe
            print(new_team_id)
            return schedule_and_record(year, new_team_id)  # pandas df
        except Exception as e2:
            print(e2)
            return None

print('running team_df')
team_df = (
        pl.from_pandas(teams_core())
        .filter(pl.col("yearID") == query_year)
        .select("teamID", "franchID")
    )  # Polars dataframe
print(f'team_df has {team_df.shape[0]} rows')

print('running teams')
teams = (
    teams_core()
    .query(f"yearID == {query_year}")[["teamID"]]
    .drop_duplicates()
    .to_numpy()
    .ravel()
)  # query on pandas df, interesting.
print(f'teams has {teams.shape[0]} rows')
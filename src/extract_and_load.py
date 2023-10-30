from pybaseball.lahman import teams_core
import pandas as pd
import polars as pl
#from prefect import task, flow
import pathlib
from secrets import randbelow
from pathlib import Path

## Manipulate sys.path
import sys
sys.path.append(str(Path(__file__).resolve().parent)) # had to do this for PyTest to work

# Import utils // helper functions
from utils.pybaseball_helpers import get_team_schedule_or_fallback
from utils.azure_funs import set_azure_details

# For azure connection
from dotenv import load_dotenv

##### Load the the environment variables #####
import os
load_dotenv()

# Get environment variables
account_name = os.getenv('ACCOUNT_NAME')
storage_account_key = os.getenv('STORAGE_ACCOUNT_KEY')
container = os.getenv('CONTAINER')


def get_team_ids(year):
    """
    Returns a Polars dataframe containing the team IDs and franchise IDs for a given year.

    Args:
        year (int): The year for which to get the team IDs.

    Returns:
        dict: A dictionary containing the team IDs and franchise IDs for a given year.
    """
    team_id_query_year = year - 1
    team_df = (
        pl.from_pandas(teams_core())
        .filter(pl.col("yearID") == team_id_query_year)
        .select("teamID", "franchID")
    )
    
    return team_df.to_pandas().set_index("teamID")["franchID"].to_dict()

def get_teams_list(team_dict):
    """
    Returns a Polars dataframe containing the team IDs and franchise IDs for a given year.

    Args:
        team_dict (dict): A dictionary containing the team IDs and franchise IDs for a given year.

    Returns:
        list[str]: A list of team IDs for the given year.
    """
    return list(team_dict.keys())


def pull_data(year: int, teams: list[str], team_ids: dict[str, str]):
    """
    Pulls data for a given year from the MLB API and returns a Polars dataframe containing the schedules for all teams.

    Args:
        year (int): The year for which to pull data.
        team_df (pl.DataFrame): A Polars dataframe containing the team IDs and franchise IDs.

    Returns:
        pl.DataFrame: A Polars dataframe containing the schedules for all teams for the given year.
    """
    all_schedules = []

    for team in teams:
        schedule_df = get_team_schedule_or_fallback(
            team, year, team_ids
        )

        if schedule_df is not None:
            schedule_df.drop("Orig. Scheduled", axis=1, inplace=True)
            all_schedules.append(schedule_df)

    return pl.from_pandas(pd.concat(all_schedules, axis=0, ignore_index=True))

def new_date_col(schedule_df: pl.DataFrame, year: int) -> pl.DataFrame:
    """
    Adds a new date column to the given Polars dataframe.

    Args:
        schedule_df (pl.DataFrame): A Polars dataframe containing the schedules for all teams for a given year.

    Returns:
        pl.DataFrame: A Polars dataframe containing the schedules for all teams for a given year, with a new date column.
    """
    schedule_df = (schedule_df
        .with_columns(Clean_Date = pl.col("Date").str.replace(r"\(\d+\)", "").str.strip_chars())
        .with_columns(Full_date = pl.col('Clean_Date') + pl.lit(f', {year}'))
        .with_columns(Date = pl.col('Full_date').str.strip_chars().str.to_date("%A, %b %d, %Y"))
        .drop('Clean_Date', 'Full_date')
    )

    return schedule_df

def fetch_data(start_year, end_year):
    final_df = pl.DataFrame()
    for year in range(start_year, end_year+1):
        print(f"Fetching data for year {year}")
        
        # Get Teams DF
        team_dict = get_team_ids(year)
        # Get Teams List
        teams = get_teams_list(team_dict)
        print(f'Total of {len(teams)} teams in {year} season to pull data for')
        # Pull the data
        schedules_df = pull_data(year, teams, team_dict)
        # Fix the dates
        schedules_df = new_date_col(schedules_df, year)
        print(f'Shape of schedules_df after date fix = {schedules_df.shape}')
        # Cast Streak column to float -- 2021 the type changes from I64 to F64 (2020 and before it was i64)
        schedules_df = schedules_df.with_columns(Streak = pl.col('Streak').cast(pl.Float64))
        # Append
        final_df = final_df.vstack(schedules_df)
        print(f'Shape of final_df = {final_df.shape}')
    return final_df

def write_to_azure(final_df: pl.DataFrame, path: str):
    """
    Writes a Polars DataFrame to Azure.

    Args:
        final_df (pl.DataFrame): The Polars DataFrame to write.
        path (str): The path to write the DataFrame to.
    """
    # Set Azure access info
    full_path, storage_options = set_azure_details(path)
    print(f'full_path = {full_path}')

    # run it
    final_df.write_delta(full_path, 
                         mode="overwrite",
                         overwrite_schema=True,
                         delta_write_options={'partition_by':['Tm']},
                         storage_options=storage_options)
    print(f'Wrote to {full_path}')


# Main Function to run
def main():
    # Arg to define the year we are querying for team performance details
    # The final dataset is 
    import sys
    if len(sys.argv) < 3:
        print("Usage: python your_script.py <start_year> <end_year>")
        sys.exit(1)
        
    # Set Years
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    
    # Fetch the data
    final_df = fetch_data(start_year, end_year)
    write_to_azure(final_df, 'data/raw/schedules/')


if __name__=='__main__':
    main()
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


def get_teams_df(year):
    """
    Returns a Polars dataframe containing the team IDs and franchise IDs for a given year.

    Args:
        year (int): The year for which to get the team IDs.

    Returns:
        pl.DataFrame: A Polars dataframe containing the team IDs and franchise IDs for the given year.
    """
    team_id_query_year = year - 1
    team_df = (
        pl.from_pandas(teams_core())
        .filter(pl.col("yearID") == team_id_query_year)
        .select("teamID", "franchID")
    )
    return team_df

def get_teams_list(team_df):
    """
    Returns a Polars dataframe containing the team IDs and franchise IDs for a given year.

    Args:
        year (int): The year for which to get the team IDs.

    Returns:
        pl.DataFrame: A Polars dataframe containing the team IDs and franchise IDs for the given year.
    """
    teams = list(
        (
        team_df
        .select('teamID')
        .unique()
        .to_numpy()
        .ravel()
        )
    )

    return teams

def pull_data(year, teams, team_df):
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
            team, year, team_df.to_pandas()
        )

        if schedule_df is not None:
            schedule_df.drop("Orig. Scheduled", axis=1, inplace=True)
            all_schedules.append(schedule_df)

    return pl.from_pandas(pd.concat(all_schedules, axis=0, ignore_index=True))


# Main Function to run
def main():
    # Arg to define the year we are querying for team performance details
    # The final dataset is 
    import sys
    year = int(sys.argv[1])
    print(f'Year = {year}')

    # Get Teams DF
    team_df = get_teams_df(year)
    print(f'Shape of team_df = {team_df.shape}')
    # Get Teams List
    teams = get_teams_list(team_df)
    print(f'Total of {len(teams)} teams in {year} season to pull data for')
    # Pull the data
    schedules_df = pull_data(year, teams, team_df)
    print(f'Shape of schedules_df = {schedules_df.shape}')

    # Set Azure access info
    full_path, storage_options = set_azure_details()
    print(f'full_path = {full_path}')

    # run it
    schedules_df.write_delta(full_path, 
                         mode="append",
                         delta_write_options={'partition_by':['Tm']},
                         storage_options=storage_options)
    print(f'Wrote to {full_path}')

if __name__=='__main__':
    main()
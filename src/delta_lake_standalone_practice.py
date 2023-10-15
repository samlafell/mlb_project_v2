from pybaseball.lahman import teams_core
import pandas as pd
import polars as pl
#from prefect import task, flow
import pathlib
from secrets import randbelow
from pathlib import Path

# Import utils
from utils.team_performance import schedule_and_record

# For azure connection
from dotenv import load_dotenv

##### Load the the environment variables #####
import os
load_dotenv()

# Get environment variables
account_name = os.getenv('ACCOUNT_NAME')
storage_account_key = os.getenv('STORAGE_ACCOUNT_KEY')
container = os.getenv('CONTAINER')

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


def pull_data(year):
    # Go one year back
    team_id_query_year = year - 1
    # Get team IDs for the year
    team_df = (
        pl.from_pandas(teams_core())
        .filter(pl.col("yearID") == team_id_query_year)
        .select("teamID", "franchID")
    )  # Polars dataframe

    teams = (
        teams_core()
        .query(f"yearID == {team_id_query_year}")[["teamID"]]
        .drop_duplicates()
        .to_numpy()
        .ravel()
    )

    # Preallocate list to collect dataframes
    all_schedules = []

    for team in teams:
        schedule_df = get_team_schedule_or_fallback(
            team, year, team_df
        )  # returns a pandas df

        if schedule_df is not None:
            ################ Drop Completely Null Column that was giving problems ################
            schedule_df.drop("Orig. Scheduled", axis=1, inplace=True)

            ## Stack the DF
            all_schedules.append(schedule_df)

            # Stop to test
            if team == 'ARI':
                break

    return pl.from_pandas(pd.concat(all_schedules, axis=0, ignore_index=True))

# # Create directory if it doesn't exist in local testing
# def create_dir_if_not_exists(dir_path: Path):
#     if not dir_path.exists():
#         dir_path.mkdir(parents=True, exist_ok=True)
#         print(f"Directory {dir_path} created.")
#     else:
#         print(f"Directory {dir_path} already exists.")

# # Set the paths
# def set_paths():
#     root = pathlib.Path(__file__).parent.parent.absolute()
#     TABLE_PATH = Path(root, 'test', "data", "raw")
#     create_dir_if_not_exists(TABLE_PATH)
#     return TABLE_PATH

# Set Azure details
def set_azure_details():
    storage_options = {
        "AZURE_STORAGE_ACCOUNT_NAME":account_name,
        "AZURE_STORAGE_ACCOUNT_KEY": storage_account_key,
        "AZURE_CONTAINER_NAME":container
    }

    table_path = "test/data/raw/schedules"
    full_path = f'abfs://{container}/{table_path}'
    return full_path, storage_options


# Main Function to run
def main():
    # Arg to define the year we are querying for team performance details
    # The final dataset is 
    import sys
    year = int(sys.argv[1])
    print(f'Year = {year}')

    # Pull the data
    schedules_df = pull_data(year)
    print(f'Shape of schedules_df = {schedules_df.shape}')

    # Set Azure access info
    full_path, storage_options = set_azure_details()
    print(f'full_path = {full_path}')

    # run it
    schedules_df.write_delta(full_path, 
                         mode="overwrite",
                         delta_write_options={'partition_by':['Tm']},
                         storage_options=storage_options)
    print(f'Wrote to {full_path}')

if __name__=='__main__':
    main()
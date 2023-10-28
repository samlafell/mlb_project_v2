from pybaseball.lahman import teams_core
from pybaseball import schedule_and_record
import pandas as pd
import polars as pl
import os
#from prefect import task, flow
import pathlib
from secrets import randbelow
from pathlib import Path

# Import utils
from utils.azure_funs import set_azure_details

## Test
test = True
if test == True:
    fail_early = True
    schedule_table_path = "test/data/raw/schedules"
else:
    fail_early = False
    schedule_table_path = "data/raw/schedules"

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

            if fail_early:
                break

    return pl.from_pandas(pd.concat(all_schedules, axis=0, ignore_index=True))

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
    full_path, storage_options = set_azure_details(schedule_table_path)
    print(f'full_path = {full_path}')

    # run it
    schedules_df.write_delta(full_path, 
                         mode="append",
                         delta_write_options={'partition_by':['Tm']},
                         storage_options=storage_options)
    print(f'Wrote to {full_path}')

if __name__=='__main__':
    main()
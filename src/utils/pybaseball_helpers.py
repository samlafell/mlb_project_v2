from pybaseball import schedule_and_record

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
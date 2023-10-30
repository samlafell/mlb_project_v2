from pybaseball import schedule_and_record
import polars as pl

def get_team_schedule_or_fallback(team, year, team_dict):
    """
    This function retrieves the schedule and record for a given team and year. If an error occurs, it will try to
    retrieve the schedule and record for a fallback team specified in the team_dict. If that also fails, it will return
    None.

    Args:
        team (str): The team name to retrieve the schedule and record for.
        year (int): The year to retrieve the schedule and record for.
        team_dict (dict): A dictionary mapping team names to fallback team IDs.

    Returns:
        pandas.DataFrame or None: The schedule and record for the given team and year, or None if it could not be
        retrieved.
    """
    try:
        return schedule_and_record(year, team)
    except Exception as e1:
        print(e1)
        try:
            new_team_id = team_dict.get(team, "default_value")
            print(new_team_id)
            return schedule_and_record(year, new_team_id)
        except Exception as e2:
            print(e2)
            return None
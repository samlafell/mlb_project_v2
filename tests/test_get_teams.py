import pytest
import polars as pl
from src.extract_and_load import get_teams_df, get_teams_list

@pytest.fixture
def teams_core():
    return pl.DataFrame({
        "yearID": [2019, 2019, 2020, 2020],
        "teamID": ["ATL", "BOS", "ATL", "BOS"],
        "franchID": ["ATL", "BOS", "ATL", "BOS"]
    })

def test_get_teams_df_returns_dataframe():
    # Arrange
    year = 2020

    # Act
    result = get_teams_df(year)

    # Assert
    assert isinstance(result, pl.DataFrame)

def test_get_teams_df_returns_expected_columns():
    # Arrange
    year = 2020

    # Act
    result = get_teams_df(year)

    # Assert
    expected_columns = ["teamID", "franchID"]
    assert result.columns == expected_columns

def test_get_teams_list_returns_list():
    # Arrange
    team_df = pl.DataFrame({
        "teamID": ["ATL", "BOS", "ATL", "BOS"]
    })

    # Act
    result = get_teams_list(team_df)

    # Assert
    assert isinstance(result, list)

def test_get_teams_list_returns_expected_teams():
    # Arrange
    team_df = pl.DataFrame({
        "teamID": ["ATL", "BOS", "ATL", "BOS"]
    })

    # Act
    result = get_teams_list(team_df)

    # Assert
    expected_teams = ["ATL", "BOS"]
    assert set(result) == set(expected_teams)
"""
Lord Stanley ETL pipeline constants
"""

import pandas as pd


COMPLETED_STATES = {"FINAL", "OFF"}

SCHEDULE_COLUMNS = {
    "id": "id",
    "gameType": "game_type",
    "gameDate": "game_date",
    "gameState": "game_state",
    "awayTeam.id": "away_team_id",
    "awayTeam.abbrev": "away_team_abbrev",
    "awayTeam.score": "away_team_score",
    "homeTeam.id": "home_team_id",
    "homeTeam.abbrev": "home_team_abbrev",
    "homeTeam.score": "home_team_score",
}

SCHEDULE_DTYPES = {
    "id": pd.StringDtype(),
    "game_type": pd.StringDtype(),
    "game_date": "datetime64[ns]",
    "game_state": pd.StringDtype(),
    "away_team_id": pd.StringDtype(),
    "away_team_abbrev": pd.StringDtype(),
    "away_team_score": pd.Int64Dtype(),
    "home_team_id": pd.StringDtype(),
    "home_team_abbrev": pd.StringDtype(),
    "home_team_score": pd.Int64Dtype(),
}

GAME_COLUMNS = {
    "id": "id",
    "gameDate": "game_date",
    "startTimeUTC": "start_time",
    "gameState": "game_state",
    "awayTeam.abbrev": "away_team_abbrev",
    "awayTeam.score": "away_team_score",
    "homeTeam.abbrev": "home_team_abbrev",
    "homeTeam.score": "home_team_score",
    "periodDescriptor.number": "period_descriptor",
    "clock.timeRemaining": "clock_time_remaining",
    "clock.inIntermission": "in_intermission",
}

GAME_DTYPES = {
    "id": pd.StringDtype(),
    "game_date": "datetime64[ns]",
    "start_time": "datetime64[ns, UTC]",
    "game_state": pd.StringDtype(),
    "away_team_abbrev": pd.StringDtype(),
    "away_team_score": pd.Int64Dtype(),
    "home_team_abbrev": pd.StringDtype(),
    "home_team_score": pd.Int64Dtype(),
    "period_descriptor": pd.StringDtype(),
    "clock_time_remaining": pd.StringDtype(),
    "in_intermission": pd.BooleanDtype(),
}

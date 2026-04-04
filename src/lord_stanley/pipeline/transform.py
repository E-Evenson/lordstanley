"""
Transformation layer for Lord Stanely ETL pipeline

Responsibilities:
    - Normalizing raw data
    - Cleaning raw data
    - Enriching raw data
Not responsible for:
    - Extracting raw data
    - Applying domain logic
"""

from typing import Any

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


def _clean_schedule(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean schedule data

    Args:
        df: Schedule data to clean

    Returns:
        Cleaned schedule dataframe
    """
    df = df.drop_duplicates(subset="id")
    df = df[SCHEDULE_COLUMNS.keys()].rename(columns=SCHEDULE_COLUMNS)
    df = df.astype(SCHEDULE_DTYPES)  # type: ignore[arg-type]
    df = df[df["game_type"] == "2"]
    df = df.sort_values("id").reset_index(drop=True)
    df[["winner_abbrev", "loser_abbrev"]] = df.apply(
        lambda row: _get_winner_loser(row), axis=1, result_type="expand"
    )
    df[["winner_abbrev", "loser_abbrev"]] = df[
        ["winner_abbrev", "loser_abbrev"]
    ].astype(pd.StringDtype())

    return df


def _get_winner_loser(row: pd.Series) -> tuple[str | None, str | None]:
    """
    Calculate the winner and loser for completed games

    Args:
        row: single row of game data

    Returns:
        Team abbreviations for the winner and loser
    """
    if row["game_state"] not in COMPLETED_STATES:
        return None, None
    if row["home_team_score"] > row["away_team_score"]:
        return row["home_team_abbrev"], row["away_team_abbrev"]

    return row["away_team_abbrev"], row["home_team_abbrev"]


def transform_season_schedule(raw_schedule: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Transform raw schedule data into a cleaned DataFrame

    Args:
        raw_schedule: Raw schedule data from extract layer

    Returns:
        Cleaned DataFrame with winner/loser columns
    """
    df = pd.json_normalize(raw_schedule)
    df = _clean_schedule(df)

    return df


def transform_game_data(raw_game: dict[str, Any]) -> pd.DataFrame:
    """
    Clean and transform raw game data

    Args:
        raw_game: raw game data in json

    Returns:
        Cleaned and transformed dataframe of game data
    """
    game_data = pd.json_normalize(raw_game)
    game_data = game_data.reindex(columns=list(GAME_COLUMNS.keys())).rename(
        columns=GAME_COLUMNS
    )

    game_data = game_data.astype(GAME_DTYPES)  # type: ignore[arg-type]
    game_data["start_time"] = (
        game_data["start_time"].dt.tz_convert("America/Edmonton").dt.strftime("%H:%M")
    )

    return game_data

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


def _clean_schedule(df: pd.DataFrame) -> pd.DataFrame:
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


def _get_winner_loser(row: pd.Series) -> tuple[int | None, int | None]:
    if row["game_state"] not in COMPLETED_STATES:
        return None, None
    if row["home_team_score"] > row["away_team_score"]:
        return row["home_team_abbrev"], row["away_team_abbrev"]
    return row["away_team_abbrev"], row["home_team_abbrev"]

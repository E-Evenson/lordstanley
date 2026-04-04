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

import logging
from typing import Any

import pandas as pd

from lord_stanley.constants import COMPLETED_STATES
from lord_stanley.pipeline.constants import (
    SCHEDULE_COLUMNS,
    SCHEDULE_DTYPES,
    GAME_COLUMNS,
    GAME_DTYPES,
)


logger = logging.getLogger(__name__)


def _get_winner_loser(game: pd.Series) -> tuple[str | None, str | None]:
    """
    Calculate the winner and loser for completed games

    Args:
        game: single game data

    Returns:
        Team abbreviations for the winner and loser
    """
    if game["game_state"] not in COMPLETED_STATES:
        winner_abbrev = loser_abbrev = None

    elif game["home_team_score"] > game["away_team_score"]:
        winner_abbrev = game["home_team_abbrev"]
        loser_abbrev = game["away_team_abbrev"]

    else:
        winner_abbrev = game["away_team_abbrev"]
        loser_abbrev = game["home_team_abbrev"]

    return winner_abbrev, loser_abbrev


def _clean_schedule(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean schedule data

    Args:
        raw_df: Schedule data to clean

    Returns:
        Cleaned schedule dataframe
    """
    logger.debug(f"Cleaning schedule data. Starting rows: {len(raw_df)}")

    cleaned_df = raw_df.copy()

    cleaned_df = cleaned_df.drop_duplicates(subset="id")
    cleaned_df = cleaned_df[SCHEDULE_COLUMNS.keys()].rename(columns=SCHEDULE_COLUMNS)
    cleaned_df = cleaned_df.astype(SCHEDULE_DTYPES)  # type: ignore[arg-type]
    cleaned_df = cleaned_df[cleaned_df["game_type"] == "2"]

    cleaned_df = cleaned_df.sort_values("id").reset_index(drop=True)

    cleaned_df[["winner_abbrev", "loser_abbrev"]] = cleaned_df.apply(
        lambda row: _get_winner_loser(row), axis=1, result_type="expand"
    )
    cleaned_df[["winner_abbrev", "loser_abbrev"]] = cleaned_df[
        ["winner_abbrev", "loser_abbrev"]
    ].astype(pd.StringDtype())

    logger.debug(f"Finished cleaning schedule data. Cleaned rows: {len(cleaned_df)}")

    return cleaned_df


def transform_season_schedule(raw_schedule: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Transform raw schedule data into a cleaned DataFrame

    Args:
        raw_schedule: Raw schedule data from extract layer

    Returns:
        Cleaned DataFrame with winner/loser columns
    """
    logger.info("Running schedule transformation")

    if not raw_schedule:
        logger.warning("raw_schedule is empty")
        raise ValueError("raw_schedule is empty")

    transformed_schedule = pd.json_normalize(raw_schedule)
    transformed_schedule = _clean_schedule(transformed_schedule)

    logger.info(
        f"Finished running schedule transformation. Total games: {len(transformed_schedule)}"
    )

    return transformed_schedule


def transform_game_data(raw_game: dict[str, Any]) -> pd.DataFrame:
    """
    Clean and transform raw game data

    Args:
        raw_game: raw game data in json

    Returns:
        Cleaned and transformed dataframe of game data
    """
    logger.info("Running game transformation")

    if not raw_game:
        logger.warning("raw_game is empty")
        raise ValueError("raw_game is empty")

    game_data = pd.json_normalize(raw_game)
    game_data = game_data.reindex(columns=list(GAME_COLUMNS.keys())).rename(
        columns=GAME_COLUMNS
    )
    game_data = game_data.astype(GAME_DTYPES)  # type: ignore[arg-type]

    game_data["start_time"] = (
        game_data["start_time"].dt.tz_convert("America/Edmonton").dt.strftime("%H:%M")
    )

    logger.info("Finished running game transformation")

    return game_data

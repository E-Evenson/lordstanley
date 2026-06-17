"""
Extraction layer for the Lord Stanley ETL pipeline

Responsibilites:
    - Fetching raw data from NHL API
    - Persisting raw schedule to disk as JSON

Not responsible for:
    - Data validation, transformation, or business logic
"""

import json
import logging
from typing import Any

import asyncio

from lord_stanley.config import RAW_DIR
from nhl_api.api import fetch_team_schedules, fetch_game_data


logger = logging.getLogger(__name__)


def _save_raw(schedule: list[dict[str, Any]], season: str) -> None:
    """
    Save raw schedule data to disk as JSON

    Args:
        schedule: Raw schedule data to save
        season: Season code, used to name the file
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    filepath = RAW_DIR / f"schedule_{season}.json"

    logger.debug(f"Writing raw data to {filepath}")
    with open(filepath, "w") as f:
        json.dump(schedule, f)


def extract_season_schedule(season: str, teams: list[str]) -> list[dict[str, Any]]:
    """
    Get the full season schedule for a list of teams for a given season
    and save to .json

    Args:
        season: The season code to get the schedule for
        teams: The list of teams to get the schedule for

    Returns:
        A list of dicts, where each dict is a team's season schedule data
    """
    logger.info(f"Extracting season schedule for {len(teams)} teams")

    full_season_schedule = asyncio.run(fetch_team_schedules(season, teams))
    _save_raw(full_season_schedule, season)

    logger.info(
        f"Schedule extraction completed, {len(full_season_schedule)} team schedules retrieved"
    )

    return full_season_schedule


def extract_single_game(game_id: str) -> dict[str, Any]:
    """
    Get the data for a given game. Raw data is not saved to disk as it is fetched
    fresh every time.

    Args:
        game_id: The id code for the desired game

    Returns:
        A dict of the data for game_id
    """
    logger.debug(f"Extracting game data for game_id: {game_id}")
    game_data = fetch_game_data(game_id)

    return game_data

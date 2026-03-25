import json
from typing import Any

import asyncio

from lord_stanley.config import RAW_DIR
from nhl_api.api import fetch_all_team_schedules


def extract_season_schedule(season: str) -> list[dict[str, Any]]:
    """
    Get the full season schedule for a given season

    Args:
        season: The season code to get the schedule of

    Returns:
        A list of dicts, where each dict is a single game's data
    """

    full_season_schedule = asyncio.run(fetch_all_team_schedules(season))
    _save_raw(full_season_schedule, season)

    return full_season_schedule


def _save_raw(schedule: list[dict[str, Any]], season: str) -> None:
    """
    Save raw schedule data to disk as JSON
    Args:
        schedule: Raw schedule data to save
        season: Season code, used to name the file
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    filepath = RAW_DIR / f"schedule_{season}.json"
    with open(filepath, "w") as f:
        json.dump(schedule, f)

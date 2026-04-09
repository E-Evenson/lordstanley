"""
NHL API client for fetching raw data from the NHL Web API.

Responsibilities:
    - HTTP requests to NHL API endpoints
    - Returning raw parsed JSON

Not responsible for:
    - Data transformation or validation
    - Business logic or domain concepts
"""

import logging
from typing import Any

import asyncio
import aiohttp  # type: ignore
import requests  # type: ignore

from nhl_api.constants import (
    WEB_API_BASE_URL,
    GAME_BOXSCORE_URL,
    TEAM_FULL_SEASON_SCHEDULE_URL,
)


logger = logging.getLogger(__name__)


_DEFAULT_TIMEOUT = 30


def _sync_fetch(url: str) -> dict[str, Any]:
    """
    Fetch data from a URL endpoint synchronously.

    Args:
        url: The URL to fetch.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        requests.HTTPError: If the server returns an HTTP error status.
        requests.exceptions.Timeout: If the request exceeds the timeout.
        requests.exceptions.RequestException: If a connection error occurs.
        ValueError: If the response is valid JSON but not a dictionary.
    """

    response = requests.get(url, timeout=_DEFAULT_TIMEOUT)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object from {url}, got {type(data).__name__}")

    return data


async def _async_fetch(session: aiohttp.ClientSession, url: str) -> dict[str, Any]:
    """
    Fetch data from a URL endpoint asynchronously.

    Args:
        session: An active aiohttp ClientSession.
        url: The URL to fetch.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        aiohttp.ClientResponseError: If the server returns an HTTP error status.
        aiohttp.ClientError: If a connection or timeout error occurs.
        ValueError: If the response is valid JSON but not a dictionary.
    """

    async with session.get(
        url, timeout=aiohttp.ClientTimeout(total=_DEFAULT_TIMEOUT)
    ) as response:
        response.raise_for_status()

        data = await response.json()

        if not isinstance(data, dict):
            raise ValueError(
                f"Expected JSON object from {url}, got {type(data).__name__}"
            )

        return data


def fetch_game_data(game_id: str) -> dict[str, Any]:
    """
    Fetch game data for a single NHL game

    Args:
        game_id: The game id to fetch

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        requests.HTTPError: If the server returns an HTTP error status.
        requests.exceptions.Timeout: If the request exceeds the timeout.
        requests.exceptions.RequestException: If a connection error occurs.
        ValueError: If the response is valid JSON but not a dictionary.
    """

    logger.debug(f"Running fetch_game_data for {game_id}")

    game_url = f"{WEB_API_BASE_URL}{GAME_BOXSCORE_URL.format(game_id=game_id)}"
    game_data = _sync_fetch(game_url)

    logger.debug("Finished running fetch_game_data")

    return game_data


async def _fetch_single_team_schedule(
    session: aiohttp.ClientSession, team: str, season: str
) -> dict[str, Any]:
    """
    Fetch a full season schedule for a single team asynchronously

    Args:
        session: async session
        team: Team tricode (e.g. CGY, DET, MTL)
        season: Season id (e.g. 20242025)

    Returns:
        A dict with a team's schedule data

    Raises:
        aiohttp.ClientResponseError: If the server returns an HTTP error status.
        aiohttp.ClientError: If a connection or timeout error occurs.
        ValueError: If the response is valid JSON but not a dictionary.
    """

    logger.debug(f"Running fetch_team_schedule for {team} in {season} season")

    url = f"{WEB_API_BASE_URL}{TEAM_FULL_SEASON_SCHEDULE_URL.format(team=team, season=season)}"

    team_schedule_data = await _async_fetch(session, url)

    if not team_schedule_data.get("games"):
        logger.warning(f"No schedule data for {team} in {season} season")

    logger.debug(f"Finished fetching team schedule for {team} in {season} season")

    return team_schedule_data


async def fetch_team_schedules(
    season: str,
    teams: list[str],
) -> list[dict[str, Any]]:
    """
    Fetch the full season schedule for each team in a list of teams

    NOTE: this will likely have duplicated games as every game has two teams, and will show up
    on each team's schedule. It will also include duplicates for any team triCode that
    appears more than once

    Args:
        season: Season to fetch team schedules for.
        teams: Teams to fetch season schedules for.

    Returns:
        A list of dicts, where each dict is a team's schedule data

    Raises:
        aiohttp.ClientResponseError: If the server returns an HTTP error status.
        aiohttp.ClientError: If a connection or timeout error occurs.
        ValueError: If the response is valid JSON but not a dictionary.

    """

    logger.info(f"Running fetch_team_schedules for {season}, for {len(teams)}")

    async with aiohttp.ClientSession() as session:
        team_schedules = await asyncio.gather(
            *(_fetch_single_team_schedule(session, team, season) for team in teams)
        )

    logger.info(f"Finished running fetch_team_schedules for {season} season.")

    return team_schedules


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    teams = asyncio.run(fetch_team_schedules("20232024", ["CGY"]))
    print(teams)

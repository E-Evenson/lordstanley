import logging
from typing import Any

import asyncio
import aiohttp  # type: ignore
import requests  # type: ignore

from nhl_api.constants import (
    STATS_API_BASE_URL,
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


def fetch_team_data() -> list[dict[str, Any]]:
    """
    Fetch basic data for all NHL teams

    This data includes:
    id,
    franchiseId,
    fullName,
    leagueId,
    rawTricode,
    triCode

    For example, Calgary Flames and Atlanta Flames would have
    different ids but the same franchise ids, because they moved from
    Atlanta to Calgary. So they are the same franchise, but a different team. While the
    Phoenix/Arizona Coyotes were franchise 28, but the Utah Mammoth are franchise 40.
    Also note that the Utah Hockey Club and Utah Mammoth are separate entries with
    different ids, but the same triCode. The Utah Mammoth is correct, with id=68

    To be run when a team moves, renames, or the league expands

    Returns:
        Parsed JSON response as a list of dictionaries

    Raises:
        ValueError: If the data key is missing
    """

    logger.info("Running fetch_team_data")
    team_data_url = f"{STATS_API_BASE_URL}/team"

    teams_data = _sync_fetch(team_data_url)
    teams_list = teams_data.get("data")

    if teams_list is None:
        raise ValueError(
            "Unexpected response structure from NHL API — 'data' key missing"
        )

    logger.info("Finished running fetch_team_data")

    return teams_list


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
) -> list[dict[str, Any]]:
    """
    Fetch a full season schedule for a single team asynchronously

    Args:
        session: async session
        team: Team tricode (e.g. CGY, DET, MTL)
        season: Season id (e.g. 20242025)

    Returns:
        A list of dicts, where each dict is a single game's data

    Raises:
        aiohttp.ClientResponseError: If the server returns an HTTP error status.
        aiohttp.ClientError: If a connection or timeout error occurs.
        ValueError: If the response is valid JSON but not a dictionary.
    """

    logger.debug(f"Running fetch_team_schedule for {team} in {season} season")

    url = f"{WEB_API_BASE_URL}{TEAM_FULL_SEASON_SCHEDULE_URL.format(team=team, season=season)}"

    team_schedule_data = await _async_fetch(session, url)

    team_games_list = team_schedule_data.get("games")
    # not all teams play in a season, remove teams with empty schedules
    if not team_games_list:
        logger.warning(f"No games found for {team} in {season}")
        return []

    logger.debug(f"Finished running fetch_team_schedule for {team} in {season} season")

    return team_games_list


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
        A list of dicts, where each dict is a single game's data

    Raises:
        aiohttp.ClientResponseError: If the server returns an HTTP error status.
        aiohttp.ClientError: If a connection or timeout error occurs.
        ValueError: If the response is valid JSON but not a dictionary.

    """

    logger.info(f"Running fetch_team_schedules for {season}")

    async with aiohttp.ClientSession() as session:
        team_schedules = await asyncio.gather(
            *(_fetch_single_team_schedule(session, team, season) for team in teams)
        )

    schedules_extracted = 0
    all_games = []
    for team in team_schedules:
        if team:
            schedules_extracted += 1
        all_games.extend(team)

    logger.info(
        f"Finished running fetch_team_schedules for {season}. {schedules_extracted} team's schedules returned"
    )
    return all_games


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    teams = fetch_game_data("2023020416")
    print(teams)

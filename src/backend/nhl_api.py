import logging
from pathlib import Path

import asyncio
import aiohttp
import pandas as pd
import requests


logger = logging.getLogger(__name__)

logger.info(f"imported {__name__}")


def refresh_team_data() -> pd.DataFrame:
    """
    Pulls basic data of all nhl teams that have ever existed.
    Data received is:
    id,
    franchiseId,
    fullName,
    leagueId,
    rawTricode,
    triCode

    For example, Calgary Flames and Atlanta Flames would have
    different ids but the same franchise ids, because they moved from
    Atlanta to Calgary. So they are the same franchise, but a different team

    This shouldn't need to be updated very often,
    but can be run whenever a new team is added or a team moves
    """

    logger.info("Running _get_team_data")
    team_data_url = "https://api.nhle.com/stats/rest/en/team"

    response = requests.get(team_data_url)

    if response.status_code != 200:
        logging.error(f"Check NHL API team endpoint {team_data_url}")
    assert response.status_code == 200, (
        "_get_team_data response status code not 200"
    )
    logger.debug(
        f"_get_team_data request response status code: {response.status_code}"
    )
    raw_team_data = response.json()

    team_data_df = pd.json_normalize(raw_team_data["data"]).dropna()

    team_data_df = team_data_df.astype(
        {
            "id": int,
            "franchiseId": int,
            "fullName": str,
            "leagueId": int,
            "rawTricode": str,
            "triCode": str,
        }
    )

    return team_data_df


def get_single_game_data(game_id: str) -> pd.DataFrame:
    """
    Pulls the data of a single game from the NHL API using the game ID
    """
    logger.info(f"Running get_game_data for {game_id}")
    game_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

    response = requests.get(game_url)
    logger.debug(f"get_game_data request response: {response}")
    game_data = response.json()

    game_data = pd.json_normalize(game_data)

    game_data.to_csv(Path.cwd() / f"data/learning/{game_id}.csv")

    logger.info("Finished running get_single_game_data")

    return game_data


async def _get_season_schedule_for_single_team(session, url):
    """
    Gets the full schedule info for a given team in a given season
    """

    logger.info(f"Calling {url}")

    response = await session.get(url)
    team_schedule = await response.json()

    return team_schedule


async def _compile_season_schedules_of_multiple_teams(
    teams: list[str],
    season: str,
):
    """
    Calls the _get_team_schedule function for each team in the supplied
    iterable asynchronously. Done asynchronously for speed
    Concatenates the results of each call into
    a single dataframe, duplicates removed, and sorted in game order
    """

    logger.info("Starting get_full_season_schedule")
    full_schedule_df = pd.DataFrame()

    async with aiohttp.ClientSession() as session:
        tasks = []

        for triCode in teams:
            schedule_url = f"https://api-web.nhle.com/v1/club-schedule-season/{triCode}/{season}"
            tasks.append(
                asyncio.ensure_future(
                    _get_season_schedule_for_single_team(session, schedule_url)
                )
            )

        gathered_schedules = await asyncio.gather(*tasks)

    games_list = []
    for team in gathered_schedules:
        df = pd.json_normalize(team["games"])
        games_list.append(df)

    full_schedule_df = pd.concat(games_list)
    full_schedule_df = (
        full_schedule_df.drop_duplicates(subset="id")
        .sort_values("id", ascending=True)
        .reset_index(drop=True)
    )

    logger.info("Finished get_full_season_schedule")

    return full_schedule_df


def get_full_schedule(season: str, teams: list = None) -> pd.DataFrame:
    """
    function to request full schedule for provided season, for given teams.
    ideally a list of teams will be passed, but if not, the function will get a list
    of every team that has existed in NHL history from the NHL API
    """

    # if teams to retreive schedule for isn't passed as an argument,
    # request full list from NHL API
    if teams is None:
        teams = list(refresh_team_data()["triCode"])

    full_schedule = asyncio.run(
        _compile_season_schedules_of_multiple_teams(teams, season)
    )

    return full_schedule


if __name__ == "__main__":
    logging.basicConfig(
        level="DEBUG",
        filename=Path.cwd() / "logs/nhl_api.log",
        filemode="w",
        format="%(asctime)s: %(levelname)s: %(message)s",
    )

    full_schedule = get_full_schedule("20242025")
    print(full_schedule)

    # team_data = refresh_team_data()
    # print(team_data)

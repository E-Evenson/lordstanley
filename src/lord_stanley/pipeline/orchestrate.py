"""
Orchestration layer for the Lord Stanley ETL pipeline

Responsibilites:
    - Orchestrate extract, transform, and load layers
    - Receive and pass arguments to ETL layers
"""

import logging

import pandas as pd

from lord_stanley.pipeline import extract, transform, load


logger = logging.getLogger(__name__)


def run_schedule_etl(season: str, teams: list[str]) -> pd.DataFrame:
    """
    Run ETL for the full season schedule for a given season and list of teams

    Args:
        season: The season code to run the ETL for
        teams: The teams to get schedules for

    Returns:
        Dataframe of transformed schedule data
    """
    logger.info(f"Running schedule ETL for {len(teams)} for the {season} season.")

    raw_schedule = extract.extract_season_schedule(season, teams)
    transformed_schedule = transform.transform_season_schedule(raw_schedule)
    load.save_schedule(transformed_schedule, season)

    logger.info(
        f"Finished running schedule ETL. Games returned: {len(transformed_schedule)}"
    )

    return transformed_schedule


def run_game_etl(game_id: str) -> pd.DataFrame:
    """
    Run ETL for a single game

    Args:
        game_id: Game ID for the game to retrieve

    Returns:
        Dataframe of transformed game data
    """
    logger.info(f"Running ETL for game: {game_id}.")

    raw_game = extract.extract_single_game(game_id)
    transformed_game = transform.transform_game_data(raw_game)

    logger.info("Finished running game ETL")

    return transformed_game


if __name__ == "__main__":
    from lord_stanley.domain.constants import ACTIVE_TEAM_TRICODES

    run_schedule_etl("20252026", ACTIVE_TEAM_TRICODES)

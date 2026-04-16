"""
Orchestration layer for Lord Stanley pipelines. The local functions are for local ETL. The BigQuery
functions are for ELT directly loading into BigQuery.

Responsibilites:
    - Orchestrate extract, transform, and load layers
    - Receive and pass arguments to ETL/ELT layers
"""

import logging

import pandas as pd

from lord_stanley.pipeline import extract, load
from lord_stanley.pipeline.transform import pandas


logger = logging.getLogger(__name__)


def run_local_schedule_etl(season: str, teams: list[str]) -> pd.DataFrame:
    """
    Run local ETL for the full season schedule for a given season and list of teams

    Args:
        season: The season code to run the ETL for
        teams: The teams to get schedules for

    Returns:
        Dataframe of transformed schedule data
    """
    logger.info(f"Running schedule ETL for {len(teams)} for the {season} season.")

    raw_schedule = extract.extract_season_schedule(season, teams)
    transformed_schedule = pandas.transform_season_schedule(raw_schedule)
    load.save_schedule(transformed_schedule, season)

    logger.info(
        f"Finished running schedule ETL. Games returned: {len(transformed_schedule)}"
    )

    return transformed_schedule


def run_bigquery_schedule_elt(season: str, teams: list[str]) -> None:
    """
    Run ELT for BigQuery for the full season schedule for a given season and list of teams

    Args:
        season: The season code to run the ETL for
        teams: The teams to get schedules for

    """
    logger.info(f"Running BigQuery ELT for {len(teams)} teams for the {season} season.")

    raw_schedule = extract.extract_season_schedule(season, teams)
    load.load_to_bigquery(raw_schedule)
    # TODO: trigger dbt transform

    logger.info(
        f"Finished running BigQuery ELT for {season} season. Schedules for {len(raw_schedule)} teams processed."
    )


def run_local_game_etl(game_id: str) -> pd.DataFrame:
    """
    Run local ETL for a single game

    Args:
        game_id: Game ID for the game to retrieve

    Returns:
        Dataframe of transformed game data
    """
    logger.info(f"Running ETL for game: {game_id}.")

    raw_game = extract.extract_single_game(game_id)
    transformed_game = pandas.transform_game_data(raw_game)

    logger.info("Finished running game ETL")

    return transformed_game


if __name__ == "__main__":
    from lord_stanley.domain.constants import ACTIVE_TEAM_TRICODES

    logging.basicConfig(level="INFO")

    run_bigquery_schedule_elt("20252026", ACTIVE_TEAM_TRICODES)

    run_local_schedule_etl("20252026", ACTIVE_TEAM_TRICODES)

"""
Orchestration layer for the Lord Stanely domain calculations

Responsibilities:
    - Which season it is
    - Which NHL teams are included
    - Which team starts with the cup
    - Ensuring schedule data exists
    - Running game ETL
    - Running schedule ETL
    - Applying domain calculations in the correct order
"""

import logging
from typing import TypedDict

import pandas as pd

from lord_stanley.config import (
    PROCESSED_DIR,
    CURRENT_SEASON,
    CUP_HOLDER_START,
    REFERENCE_DATA_DIR,
)
from lord_stanley.domain.constants import ACTIVE_TEAM_TRICODES, COMPLETED_GAME_STATES
from lord_stanley.pipeline import orchestrate as pipeline
from lord_stanley.domain import (
    cup_possession,
    assign_owners,
    stats_calculator,
)


logger = logging.getLogger(__name__)


def _get_next_game_data(cup_schedule: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    Find next cup game and run game ETL for it

    Args:
        cup_schedule: schedule of cup games

    Returns:
        Next game data and next game state
    """
    next_cup_game = cup_schedule.tail(1)
    next_cup_game_id = next_cup_game["id"].item()
    raw_next_game_data = pipeline.run_game_etl(next_cup_game_id)
    next_game_state = raw_next_game_data["game_state"].item()

    return raw_next_game_data, next_game_state


class LeagueCalculationsResult(TypedDict):
    """
    Return type for run_league_calculations.
    """

    league_standings: pd.DataFrame
    team_stats: pd.DataFrame
    cumulative_owner_stats: pd.DataFrame
    next_game: pd.DataFrame
    next_game_state: str
    draft: pd.DataFrame


def run_league_calculations() -> LeagueCalculationsResult:
    """
    Orchestrate all domain logic and calculations for Lord Stanley

    Returns:
        Typed dict containing league calculations
    """
    logger.info("Running league calculations.")

    logger.debug(f"Checking schedule data for {CURRENT_SEASON}")
    schedule_path = PROCESSED_DIR / f"{CURRENT_SEASON}_schedule.parquet"
    if not schedule_path.exists():
        logger.debug(f"No schedule data found for {CURRENT_SEASON} season")
        schedule = pipeline.run_schedule_etl(CURRENT_SEASON, ACTIVE_TEAM_TRICODES)
    else:
        logger.debug(f"Reading schedule data for {CURRENT_SEASON} season")
        schedule = pd.read_parquet(schedule_path)

    cup_schedule = cup_possession.get_cup_games(schedule, CUP_HOLDER_START)

    draft_path = REFERENCE_DATA_DIR / f"drafts/{CURRENT_SEASON}.csv"
    draft = pd.read_csv(draft_path)

    next_game, next_game_state = _get_next_game_data(cup_schedule)

    if next_game_state in COMPLETED_GAME_STATES:
        schedule = pipeline.run_schedule_etl(CURRENT_SEASON, ACTIVE_TEAM_TRICODES)
        cup_schedule = cup_possession.get_cup_games(schedule, CUP_HOLDER_START)
        next_game, next_game_state = _get_next_game_data(cup_schedule)

    owners_assigned = assign_owners.assign_owners(cup_schedule, draft)
    completed_cup_games_with_owners = owners_assigned.head(-1)

    league_standings = stats_calculator.calculate_league_standings(
        completed_cup_games_with_owners, draft
    )
    team_stats = stats_calculator.calculate_team_stats(
        completed_cup_games_with_owners, draft
    )
    cumulative_owner_stats = stats_calculator.calculate_cumulative_owner_stats(
        completed_cup_games_with_owners, draft
    )

    display_data: LeagueCalculationsResult = {
        "league_standings": league_standings,
        "team_stats": team_stats,
        "cumulative_owner_stats": cumulative_owner_stats,
        "next_game": next_game,
        "next_game_state": next_game_state,
        "draft": draft,
    }

    logger.info("Finished running league calculations.")

    return display_data


if __name__ == "__main__":
    display_data = run_league_calculations()
    print(display_data["league_standings"])
    print(display_data["next_game"])

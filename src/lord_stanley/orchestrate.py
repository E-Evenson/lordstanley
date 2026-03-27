import logging

import pandas as pd

from lord_stanley.config import (
    PROCESSED_DIR,
    CURRENT_SEASON,
    CUP_HOLDER_START,
    REFERENCE_DATA_DIR,
)
from lord_stanley.pipeline import orchestrate as pipeline_orchestrate
from lord_stanley.domain import (
    cup_possession,
    assign_owners,
    next_game_formatter,
    stats_calculator,
)


logger = logging.getLogger(__name__)


NON_COMPLETED_GAME_STATES = [
    "FUT",
    "LIVE",
    "CRIT",
]


def _get_next_game_data(cup_schedule: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    next_cup_game = cup_schedule.tail(1)
    next_cup_game_id = next_cup_game["id"].item()
    raw_next_game_data = pipeline_orchestrate.run_game_etl(next_cup_game_id)
    next_game_state = raw_next_game_data["game_state"].item()

    return raw_next_game_data, next_game_state


def run():

    schedule_path = PROCESSED_DIR / f"{CURRENT_SEASON}_schedule.parquet"
    schedule = pd.read_parquet(schedule_path)
    cup_schedule = cup_possession.get_cup_games(schedule, CUP_HOLDER_START)

    draft_path = REFERENCE_DATA_DIR / f"drafts/{CURRENT_SEASON}.csv"
    draft = pd.read_csv(draft_path)

    next_game_data, next_game_state = _get_next_game_data(cup_schedule)

    if next_game_state not in NON_COMPLETED_GAME_STATES:
        schedule = pipeline_orchestrate.run_schedule_etl(CURRENT_SEASON)
        cup_schedule = cup_possession.get_cup_games(schedule, CUP_HOLDER_START)
        next_game_data, next_game_state = _get_next_game_data(cup_schedule)

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
    if next_game_state in [
        "LIVE",
        "CRIT",
    ]:
        next_game = next_game_formatter.format_live_game(next_game_data, draft)
    else:
        next_game = next_game_formatter.format_future_game(next_game_data, draft)

    display_data = {
        "league_standings": league_standings,
        "team_stats": team_stats,
        "cumulative_owner_stats": cumulative_owner_stats,
        "next_game": next_game,
    }

    return display_data


if __name__ == "__main__":
    display_data = run()
    print(display_data)

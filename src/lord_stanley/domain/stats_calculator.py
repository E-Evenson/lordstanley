import time
import logging

import pandas as pd


def calculate_league_standings(
    cup_games_with_owners: pd.DataFrame, draft: pd.DataFrame
) -> pd.DataFrame:

    win_counts = cup_games_with_owners["winner_owner"].value_counts()
    games_played = win_counts.add(
        cup_games_with_owners["loser_owner"].value_counts(), fill_value=0
    )

    stats = (
        pd.DataFrame(
            {
                "points": win_counts,
                "games_played": games_played,
            }
        )
        .reset_index()
        .rename(columns={"index": "owner"})
    )

    owners = draft[["owner"]].drop_duplicates()

    standings = pd.merge(left=owners, right=stats, how="left", on="owner").fillna(0)

    standings = standings.sort_values(
        ["points", "games_played"], ascending=[False, True]
    )

    standings.index = pd.RangeIndex(start=1, stop=len(standings) + 1)
    standings.index.name = "position"

    return standings


def calculate_team_stats(
    cup_games_with_owners: pd.DataFrame, draft: pd.DataFrame
) -> pd.DataFrame:

    win_counts = cup_games_with_owners["winner_abbrev"].value_counts()
    games_played = win_counts.add(
        cup_games_with_owners["loser_abbrev"].value_counts(), fill_value=0
    )

    team_stats = (
        pd.DataFrame(
            {
                "points": win_counts,
                "games_played": games_played,
            }
        )
        .reset_index()
        .rename(columns={"index": "team_abbrev"})
    )

    team_stats_with_owners = pd.merge(
        draft, team_stats, how="left", on="team_abbrev"
    ).fillna(0)
    ranked_team_stats_with_owners = team_stats_with_owners.sort_values(
        ["owner", "points", "games_played"], ascending=[True, False, True]
    )

    ranked_team_stats_with_owners["position"] = (
        ranked_team_stats_with_owners.groupby("owner").cumcount() + 1
    )
    return ranked_team_stats_with_owners


if __name__ == "__main__":
    from pathlib import Path
    from lord_stanley.pipeline import orchestrate
    from lord_stanley.domain import cup_possession, assign_owners
    from lord_stanley.config import CURRENT_SEASON, CUP_HOLDER_START

    t0 = time.perf_counter()
    draft = pd.read_csv(Path().cwd() / "reference_data/drafts/20252026.csv")
    t1 = time.perf_counter()
    schedule = orchestrate.run_schedule_etl(CURRENT_SEASON)
    t2 = time.perf_counter()
    schedule = pd.read_parquet(
        Path().cwd() / f"data/processed/{CURRENT_SEASON}_schedule.parquet"
    )
    t3 = time.perf_counter()
    cup_games = cup_possession.get_cup_games(schedule, CUP_HOLDER_START)
    owners_assigned = assign_owners.assign_owners(cup_games, draft)
    standings = calculate_league_standings(owners_assigned, draft)
    t6 = time.perf_counter()
    print(standings)
    print(f"etl time: {t2 - t1}")
    print(f"parquet time: {t3 - t2}")
    print(f"Time after retrieving raw schedule: {t6 - t3}")
    team_stats = calculate_team_stats(owners_assigned, draft)
    print(team_stats)

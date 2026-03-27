import logging

import pandas as pd


def calculate_league_standings(
    completed_cup_games_with_owners: pd.DataFrame, draft: pd.DataFrame
) -> pd.DataFrame:

    win_counts = completed_cup_games_with_owners["winner_owner"].value_counts()
    games_played = win_counts.add(
        completed_cup_games_with_owners["loser_owner"].value_counts(), fill_value=0
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
    completed_cup_games_with_owners: pd.DataFrame, draft: pd.DataFrame
) -> pd.DataFrame:

    win_counts = completed_cup_games_with_owners["winner_abbrev"].value_counts()
    games_played = win_counts.add(
        completed_cup_games_with_owners["loser_abbrev"].value_counts(), fill_value=0
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


def calculate_cumulative_owner_stats(
    completed_cup_games_with_owners: pd.DataFrame, draft: pd.DataFrame
) -> pd.DataFrame:

    owners_cumulative_stats = completed_cup_games_with_owners.copy()

    owners_cumulative_stats = pd.melt(
        owners_cumulative_stats,
        "game_date",
        ["winner_owner", "loser_owner"],
        var_name="result",
        value_name="owner",
    ).sort_values("game_date")

    owners_cumulative_stats["is_win"] = (
        owners_cumulative_stats["result"] == "winner_owner"
    )

    owners_cumulative_stats["owner_cumulative_wins"] = (
        owners_cumulative_stats["is_win"]
        .groupby(owners_cumulative_stats["owner"])
        .cumsum()
        .astype(pd.Int64Dtype())
    )

    owners_cumulative_stats["owner_cumulative_games_played"] = (
        owners_cumulative_stats.groupby("owner").cumcount() + 1
    ).astype(pd.Int64Dtype())

    owners = draft[["owner"]].drop_duplicates()
    all_cup_game_dates = completed_cup_games_with_owners[
        ["game_date"]
    ].drop_duplicates()

    full_game_grid = pd.merge(owners, all_cup_game_dates, how="cross").sort_values(
        "game_date"
    )

    owners_cumulative_stats = pd.merge(
        full_game_grid,
        owners_cumulative_stats,
        how="left",
        on=["game_date", "owner"],
    )

    owners_cumulative_stats[
        ["owner_cumulative_wins", "owner_cumulative_games_played"]
    ] = (
        owners_cumulative_stats[
            ["owner_cumulative_wins", "owner_cumulative_games_played"]
        ]
        .fillna(0)
        .ffill()
    )

    return owners_cumulative_stats

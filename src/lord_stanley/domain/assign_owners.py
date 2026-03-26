import logging

import pandas as pd


def assign_owners(schedule: pd.DataFrame, draft: pd.DataFrame) -> pd.DataFrame:
    """
    Assign owners to cup possession data

    Args:
        schedule: DataFrame containing the schedule data
        draft: DataFrame containing the draft data

    Returns:
        Schedule DataFrame with home owner, away owner, winner owner, and loser owner
    """

    owner_map = draft.set_index("team_abbrev")["owner"]

    df = schedule.copy()
    df["home_owner"] = df["home_team_abbrev"].map(owner_map)
    df["away_owner"] = df["away_team_abbrev"].map(owner_map)
    df["winner_owner"] = df["winner_abbrev"].map(owner_map)
    df["loser_owner"] = df["loser_abbrev"].map(owner_map)

    return df

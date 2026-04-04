"""
Fantasy owner calculation layer for Lord Stanley domain calculations

Responsibilities:
    - Mapping fantasy owner data to a season schedule

Not responsible for:
    - calculating owner or team stats
"""

import logging

import pandas as pd


def assign_owners(schedule: pd.DataFrame, draft: pd.DataFrame) -> pd.DataFrame:
    """
    Map fantasy owners to schedule data

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

"""
Cup possession layer for the Lord Stanley domain calculations

Responsibilities:
    - Calculating cup possession through a season

Not responsible for:
    - what season it is
    - which fantasy owners are involved
"""

import pandas as pd


def get_cup_games(
    schedule: pd.DataFrame,
    cup_holder_start: str,
) -> pd.DataFrame:
    """
    Calculate cup possession for each game in the schedule

    Args:
        schedule: Cleaned schedule DataFrame from transform layer
        cup_holder_start: which team starts with the cup

    Returns:
        Schedule DataFrame with is_cup_game column added
    """
    df = schedule.copy()
    df["is_cup_game"] = False
    cup_holder = cup_holder_start

    for i, game in df.iterrows():
        if cup_holder in (game["home_team_abbrev"], game["away_team_abbrev"]):
            df.loc[i, "is_cup_game"] = True
            if pd.isna(game["winner_abbrev"]):  # type: ignore[call-overload]
                break
            cup_holder = game["winner_abbrev"]

    df = df[df["is_cup_game"]]
    return df

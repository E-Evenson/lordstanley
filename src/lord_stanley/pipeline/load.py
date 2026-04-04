"""
Load layer for the Lord Stanley ETL pipeline

Responsibilities:
    - Save tranformed data to disk
    - Folder location
    - File naming
    - File type

Not responsible for:
    - What data to save
"""

import pandas as pd

from lord_stanley.config import PROCESSED_DIR


def save_schedule(df: pd.DataFrame, season: str) -> None:
    """
    Save processed schedule data to disk

    Args:
        df: processed schedule dataframe
        season: season id code for file naming
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_DIR / f"{season}_schedule.csv", index=False)
    df.to_parquet(PROCESSED_DIR / f"{season}_schedule.parquet")

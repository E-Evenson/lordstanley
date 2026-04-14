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

import logging
from typing import Any

from google.cloud import bigquery
import pandas as pd

from lord_stanley.config import PROCESSED_DIR
from lord_stanley.storage.bigquery import load_raw


logger = logging.getLogger(__name__)


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


def load_to_bigquery(raw_schedule: list[dict[str, Any]]) -> None:
    """
    Load raw schedule data to BigQuery

    Args:
        df: raw schedule data
    """
    logger.info("Writing raw schedule to BigQuery")

    table_id = "lord-stanley.raw.schedule"

    load_raw(raw_schedule, table_id)

    logger.info("Finished loading to BigQuery")

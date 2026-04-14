"""
Storage layer for BigQuery to read and write tables
"""

import logging
from typing import Any

from google.cloud import bigquery
import pandas as pd


logger = logging.getLogger(__name__)


def load_raw(raw_data: list[dict[str, Any]], table_id: str) -> None:
    """
    Loads raw data into BigQuery

    Args:
        raw_data: raw data to be loaded to bigquery
        table_id: table to load raw data to
    """
    logger.info(f"Writing to BigQuery table: {table_id}")

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True
    )
    job = client.load_table_from_json(raw_data, table_id, job_config=job_config)
    job.result()

    logger.info(f"Finished writing {len(raw_data)} rows to BigQuery")

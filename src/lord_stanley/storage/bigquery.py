"""
Storage layer for BigQuery to read and write tables
"""

import logging
from typing import Any

from google.cloud import bigquery
import pandas as pd


logger = logging.getLogger(__name__)


def load_json(raw_data: list[dict[str, Any]], table_id: str) -> None:
    """
    Loads raw json data into BigQuery

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


def load_df(df: pd.DataFrame, table_id: str) -> None:
    """
    Loads dataframe into BigQuery

    Args:
        df: dataframe to be loaded
        table_id: table to load dataframe to
    """
    logger.info(f"Writing dataframe to BigQuery table: {table_id}")

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logger.info(f"Finished writing {len(df)} rows to BigQuery")


def read_table(table_id: str) -> pd.DataFrame:
    """
    Read the contents of a table from BigQuery

    Args:
        - table_id: table to read data from

    Returns:
        Dataframe with read data
    """
    logger.info(f"Reading from table {table_id}")

    client = bigquery.Client()

    data = client.query(f"SELECT * FROM `{table_id}`").to_dataframe()

    logger.info(f"Read data from {table_id}. Rows returned: {len(data)}")

    return data

"""
Storage layer for BigQuery to read and write tables
"""

import json
import logging
import os
from typing import Any

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


logger = logging.getLogger(__name__)


def _init_client() -> bigquery.Client:
    """
    Initialize BigQuery Client
    """
    creds_raw = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    # Check if creds_raw is JSON
    if creds_raw and creds_raw.startswith("{"):
        logger.info("Initializing BigQuery client with JSON credentials")
        info = json.loads(creds_raw)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info["project_id"])

    # If creds_raw is filepath
    logger.info("Initializing BigQuery with filepath credentials")
    return bigquery.Client()


# Lazily initialized to avoid credential errors on import
_client = None


def _get_client() -> bigquery.Client:
    """
    Return BigQuery client, initializing it on first call
    """
    global _client
    if _client is None:
        _client = _init_client()

    return _client


def load_json(raw_data: list[dict[str, Any]], table_id: str) -> None:
    """
    Loads raw json data into BigQuery

    Args:
        raw_data: raw data to be loaded to bigquery
        table_id: table to load raw data to
    """
    logger.info(f"Writing to BigQuery table: {table_id}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True
    )
    job = _get_client().load_table_from_json(raw_data, table_id, job_config=job_config)
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

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True
    )
    job = _get_client().load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logger.info(f"Finished writing {len(df)} rows to BigQuery")


def query(sql: str) -> pd.DataFrame:
    """
    Read the contents of a table from BigQuery

    Args:
        - sql: sql query to run

    Returns:
        Dataframe with query results
    """
    logger.info("Querying data from BigQuery")

    data = _get_client().query(sql).to_dataframe()

    logger.info(f"Read data from BigQuery. Rows returned: {len(data)}")

    return data

"""
Orchestration for Lord Stanley assets
"""

import dagster as dg

from lord_stanley.config import CURRENT_SEASON
from lord_stanley.domain.constants import ACTIVE_TEAM_TRICODES
from lord_stanley.pipeline.extract import extract_season_schedule
from lord_stanley.pipeline.load import load_to_bigquery
from lord_stanley.pipeline.transform.dbt import run_stg_schedule


@dg.asset
def raw_schedule() -> None:
    """
    Run the NHL schedule extract and load raw json into BigQuery
    """
    schedule = extract_season_schedule(CURRENT_SEASON, ACTIVE_TEAM_TRICODES)
    load_to_bigquery(schedule)


@dg.asset(deps=[raw_schedule])
def cleaned_schedule() -> None:
    """
    Run the stg_schedule model
    """
    run_stg_schedule()

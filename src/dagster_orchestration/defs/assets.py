"""
Orchestration for Lord Stanley assets
"""

import dagster as dg

from lord_stanley.config import CURRENT_SEASON, CUP_HOLDER_START

from lord_stanley.pipeline.extract import extract_season_schedule
from lord_stanley.pipeline.load import load_to_bigquery
from lord_stanley.pipeline.transform.dbt import run_stg_schedule

from lord_stanley.storage.bigquery import read_table, load_df

from lord_stanley.domain.constants import ACTIVE_TEAM_TRICODES
from lord_stanley.domain.cup_possession import get_cup_games
from lord_stanley.domain.dbt import run_mart_models


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


@dg.asset(deps=[cleaned_schedule])
def cup_possession() -> None:
    """
    Run the cup possession logic in Python
    """
    schedule = read_table("lord-stanley.staging.stg_schedule")
    cup_games = get_cup_games(schedule, CUP_HOLDER_START)
    load_df(cup_games, "lord-stanley.intermediate.int_cup_possession")


@dg.asset(deps=[cup_possession])
def marts() -> None:
    """
    Run the dbt to create the marts from the staging tables
    """
    run_mart_models()

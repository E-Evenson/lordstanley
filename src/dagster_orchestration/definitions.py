"""
Dagster definitions for Lord Stanley orchestration
"""

from dagster import Definitions

from lord_stanley.logging_config import configure_logging

from dagster_orchestration.defs.assets import (
    raw_schedule,
    cleaned_schedule,
    cup_possession,
    marts,
)

configure_logging()

defs = Definitions(assets=[raw_schedule, cleaned_schedule, cup_possession, marts])

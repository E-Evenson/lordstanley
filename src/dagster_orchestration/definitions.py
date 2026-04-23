from dagster import Definitions

from dagster_orchestration.defs.assets import (
    raw_schedule,
    cleaned_schedule,
    cup_possession,
    marts,
)

defs = Definitions(assets=[raw_schedule, cleaned_schedule, cup_possession, marts])

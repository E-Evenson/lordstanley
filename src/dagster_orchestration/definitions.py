from dagster import Definitions

from dagster_orchestration.defs.assets import raw_schedule, cleaned_schedule

defs = Definitions(assets=[raw_schedule, cleaned_schedule])

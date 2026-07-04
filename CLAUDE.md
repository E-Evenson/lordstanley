# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Lord Stanley is a fantasy hockey league tracker. Four owners each draft eight NHL teams; a single "cup" moves between NHL teams based on game outcomes (the winner of any cup game holds it until their next loss), and owners accumulate points while a team they drafted holds the cup. Live NHL data is pulled from the NHL API and served through a Flask web app.

## Commands

Dependencies are managed with Poetry; prefix everything with `poetry run`.

```bash
poetry install                                          # install dependencies
poetry run flask --app src/lord_stanley/web/app.py run  # run the web app
poetry run pytest                                       # run Python tests
poetry run pytest tests/lord_stanley/pipeline/test_transform.py::test_name  # single test
poetry run mypy src                                     # type check

# Scheduled (BigQuery/dbt/Dagster) pipeline only:
poetry run dbt deps          # install dbt packages (run from dbt/ or with DBT_PROJECT_DIR set)
poetry run dagster dev       # run Dagster locally to refresh BigQuery data
poetry run dbt test          # run dbt data tests (from dbt/)
```

## Two parallel pipelines

The app has two intentionally separate data paths, switched by the `PIPELINE_RUN_METHOD` env var (`"live"` — the default and the deployed production path — or `"scheduled"`):

1. **Live pipeline** (pandas + parquet, no external services):
   `nhl_api` → `pipeline` (extract/transform/load) → `domain` → `web`.
   Entry point: `lord_stanley.domain.orchestrate.run_live_league_calculations()`.
2. **Scheduled pipeline** (ELT via Dagster + BigQuery + dbt):
   `nhl_api` → BigQuery `raw` → dbt `staging` → **Python cup-possession logic** (stateful, cannot be SQL) → BigQuery `intermediate` → dbt `mart` → `web`.
   Dagster entry point: `src/dagster_orchestration/definitions.py`; the web app reads marts via `run_league_calculations_sql()`.

Do not merge or cross-wire the two paths; their separation is a deliberate design decision.

## Architecture rules

- **Dependency direction is one-way**: `nhl_api` → `pipeline` → `domain` → `web`. The NHL API client (`src/nhl_api/`) returns raw JSON only and knows nothing about the domain; the domain knows nothing about Flask. Keep these boundaries clean — this is the stated primary architectural goal.
- **Domain functions accept and return DataFrames, never file paths.** All I/O lives at the pipeline boundary (`pipeline/extract.py`, `pipeline/load.py`) or in `storage/`.
- **Presentation logic lives in `web/formatters.py`** (column renaming, HTML rendering) — not in the domain layer.
- **Cup possession (`domain/cup_possession.py`) is order-dependent state**: it iterates the schedule row-by-row, passing the cup to each game's winner. Schedule ordering must be preserved everywhere it's consumed (this was the subject of a past bug fix for the BigQuery path).
- **Configuration is centralized** in `src/lord_stanley/config.py` (season, initial cup holder, directories, run method) and `src/lord_stanley/logging_config.py` (applied once per entry point). Env vars are documented in `.env.example`.
- **Parquet is the source of truth** for processed data; the CSV written alongside it is a debugging convenience only.

## League/reference data

- Live pipeline drafts: `reference_data/drafts/{season}.csv` (`team_abbrev`, `owner`). League structure (owner count, teams per owner) is derived entirely from this file — never hardcode it.
- Scheduled pipeline drafts: `dbt/seeds/drafts.csv` (adds a `season` column).
- Owner assignments are joined in memory at runtime, never persisted with schedule data.

## Testing

- Python tests use pytest with JSON fixture data in `tests/data/` (game states and raw schedule), loaded via fixtures in `tests/conftest.py`. Several fixtures there are stubs awaiting implementation.
- Coverage is early-stage: `pipeline/transform` has partial coverage; domain logic (including cup possession) is untested in Python.
- dbt data tests live in `dbt/tests/` as singular tests (e.g., total points must equal total cup games) and are considered complete for the dbt side.

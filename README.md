# Lord Stanley

**[lordstanley-production.up.railway.app](https://lordstanley-production.up.railway.app)** · [GitHub](https://github.com/E-Evenson/lordstanley)

A fantasy hockey league tracker built on live NHL data. Four owners each draft eight NHL teams. One Stanley Cup moves between teams based on game outcomes — the winner of any cup game keeps it until their next loss. Points accumulate as long as you hold the cup. This project pulls data from the NHL API, processes it through a structured pipeline, and serves standings and stats through a Flask web app.

---

## Architecture

There are two parallel pipelines (a live updating one and a scheduled refresh), each with a deliberate dependency direction:

```
dbt/
├── macros/           # Custom macros (generate_schema_name override)
├── models/
    ├── intermediate/ # Joined and prepared data
    ├── mart/         # Final fact tables for Flask to query
    └── staging/      # Cleaned source data
├── seeds/            # Reference data (drafts CSV)
└── tests/            # Singular data tests
src/
├── dagster_orchestration/
    ├── defs/         # Asset definitions
    ├── sql/          # BigQuery read queries
    └── definitions.py  # Dagster entry point
├── nhl_api/          # Standalone API client — raw JSON, no transformation
└── lord_stanley/
    ├── domain/       # Business logic: cup possession, owner assignment, stats
    ├── pipeline/     # Extract, transform, load, orchestrate
    ├── storage/      # Read and write across data storage backends
    └── web/          # Flask app, formatters, and templates
```

### Live update pipeline
Data flow direction: `nhl_api` -> `pipeline` -> `domain` -> `web`. The domain layer owns all business logic including decisions about when to trigger ETL. The domain orchestrator calls the pipeline. The formatter functions are where column renaming and HTML rendering happens, keeping presentation logic out of the domain.

This structure means each layer can be reasoned about independently. The NHL API client knows nothing about cup possession. The domain layer knows nothing about Flask. Keeping those boundaries clean is the main architectural goal.

### Scheduled refresh pipeline
The scheduled pipeline is orchestrated by Dagster and follows an ELT pattern. Data extraction and loading lives in the Python pipeline. Cleaning transformations are done by dbt in BigQuery. Domain logic is split between Python (for cup possession logic, as it is a stateful calculation) and dbt/BigQuery (for aggregations and standings).
Data flow: `nhl_api` -> `BigQuery raw` -> `BigQuery staging` -> `Python (cup possession)` -> `BigQuery intermediate` -> `BigQuery marts` -> `web`

---

## Key Technical Decisions

**The NHL API client is internal to the project.** It lives in `src/nhl_api/` rather than a separate package. The abstraction exists because it's useful — it isolates HTTP concerns and raw JSON handling from the rest of the code — but extracting it into its own installable library would be premature. There's one consumer, and that consumer is this project.

**Schedule data is not persisted with owner assignments.** The NHL schedule is league-agnostic; which owner holds which team is a runtime concern. Joining draft data at persist time would create output files that are tightly coupled to a specific season and league configuration. Instead, the pipeline stores clean schedule data and the domain layer joins owner information in memory when it's needed. This keeps the pipeline reusable and avoids a file explosion if the project ever supports multiple leagues or seasons.

**Standings are computed on the fly, not cached.** The inputs (game results and draft assignments) are small enough that recomputing standings on each request is trivial. Caching computed standings would add complexity without a meaningful performance benefit at this scale.

**Parquet and CSV dual storage.** Processed data is written as both parquet (dtype-safe, used at runtime) and CSV (human-readable, useful for debugging and spot-checking). The parquet file is the source of truth; the CSV is a development convenience.

**Domain functions accept DataFrames, not file paths.** I/O belongs at the pipeline boundary. By the time data reaches the domain layer it's already loaded; domain functions receive DataFrames and return DataFrames. This keeps the separation between "getting data" and "doing something with data" clean and explicit.

**Deployed app.** The local Python pipeline (pandas + parquet) is the production path for the live app. It's fast and simple, with no external dependencies.

**Scheduled refresh app.** The scheduled refresh is orchestrated by Dagster and uses an ELT pattern instead of an ETL pattern. This is to preserve raw data, allowing it to be reprocessed without the need for the API calls. If the downstream logic changes, the pipeline can be rerun on the raw data. Data is saved on BigQuery. Cup possession logic lives in Python because it is a stateful calculation. The remainder of the transformation logic is run via dbt on BigQuery. The delay when querying the data marts in BigQuery is noticeable when loading the webapp, but was done for the learning experience. If done again, I would substitute BigQuery for a local implementation of DuckDB to reduce that latency.

**Dual path pipeline.** The live and scheduled implementations are intentionally kept separate. The local version allows for quick live updates, the BigQuery/dbt version is designed for daily refreshes where initial processing latency doesn't matter. 

**Centralized logging configuration.** The logging settings are set in a centralized logging configuration module, applied once at each entry point. Includes a custom UTC ISO timestamp formatter for consistency.

---

## The Rebuild

An earlier version of this project worked but had accumulated enough technical debt that doing anything with it was painful. Logic was scattered across layers, data flow was hard to follow, and dependencies were all over the place.

Rather than patch it, I rebuilt from scratch with explicit goals: clean layer boundaries, a consistent dependency direction, and a single centralized configuration location. The rebuild was also a chance to think deliberately about architecture — what to compute versus persist, where each piece of logic belongs, and which abstractions were actually earning their keep.

The original version was how I learned the domain. The rebuilt version is how I applied what I learned.

---

## Stack

| Concern | Tool |
|---|---|
| Language | Python 3.13 (pyenv) |
| Dependencies | Poetry |
| Orchestration | Dagster |
| Data processing | pandas, dbt |
| Data warehouse | BigQuery |
| Web framework | Flask |
| Charting | Plotly |
| Deployment | Railway |

---

## Running Locally

### Live updating
```bash
# Clone and install
git clone https://github.com/E-Evenson/lordstanley
cd lordstanley
poetry install

# Run the app
poetry run flask --app src/lord_stanley/web/app.py run
```

Draft data lives in `reference_data/drafts/{season}.csv` with `team_abbrev` and `owner` columns. The number of owners and teams per owner is determined entirely by this file — the app doesn't hardcode league structure. Season and initial cup holder are configured in `src/lord_stanley/config.py`.

### Scheduled refresh
The scheduled pipeline requires a BigQuery project with `raw`, `staging`, `intermediate`, and `mart` datasets, a Google service account credentials file, and the environment variables in `.env.example`.

Draft data lives in `dbt/seeds/drafts.csv` with `team_abbrev`, `owner`, and `season` columns. This allows for multiple draft seasons from a single source.

To run the scheduled refresh the environment variable `PIPELINE_RUN_METHOD` must be set to "scheduled"

```bash
# Install dbt packages
poetry run dbt deps

# Run Dagster (this can be run on a schedule or run manually to refresh the data)
poetry run dagster dev

# Run the app (Dagster and the app must be run in separate terminals)
poetry run flask --app src/lord_stanley/web/app.py run
```

---

## What's Next

- **Tests**
    - Live updating: Live updating tests are very early stages. Some fixture data exists. Pipeline transform has partial coverage. Domain logic is untested.
    - Scheduled refresh: Testing for the dbt portion of the scheduled refresh pipeline is done. Still need tests for cup possession logic in Python.
- **Documentation** - module-level docstrings are complete; inline comments for non-obvious logic remain
- **Dockerizing** - For deployment simplicity and for the learning experience
- **DuckDB Option** - Under consideration as an alternative to BigQuery for a scheduled refresh deployment option

---

## Acknowledgements

NHL API Endpoints documented by Drew Hynes: https://gitlab.com/dword4/nhlapi
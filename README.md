# Lord Stanley

**[lordstanley-production.up.railway.app](https://lordstanley-production.up.railway.app)** · [GitHub](https://github.com/E-Evenson/lordstanley)

A fantasy hockey league tracker built on live NHL data. Four owners each draft eight NHL teams. One Stanley Cup moves between teams based on game outcomes — the winner of any cup game keeps it until their next loss. Points accumulate as long as you hold the cup. This project pulls data from the NHL API, processes it through a structured pipeline, and serves standings and stats through a Flask web app.

---

## Architecture

The project is organized into four layers with a deliberate dependency direction:

```
src/
├── nhl_api/          # Standalone API client — raw JSON, no transformation
└── lord_stanley/
    ├── pipeline/     # Extract, transform, load, orchestrate
    ├── domain/       # Business logic: cup possession, owner assignment, stats
    └── web/          # Flask app, formatters, and templates
```

Data flows in one direction: `nhl_api` → `pipeline` → `domain` → `web`. The domain layer owns all business logic including decisions about when to trigger ETL — it calls down into the pipeline, not the other way around. The formatter functions are where column renaming and HTML rendering happens, keeping presentation logic out of the domain.

This structure means each layer can be reasoned about independently. The NHL API client knows nothing about cup possession. The domain layer knows nothing about Flask. Keeping those boundaries clean is the main architectural goal.

---

## Key Technical Decisions

**The NHL API client is internal to the project.** It lives in `src/nhl_api/` rather than a separate package. The abstraction exists because it's useful — it isolates HTTP concerns and raw JSON handling from the rest of the code — but extracting it into its own installable library would be premature. There's one consumer, and that consumer is this project.

**Schedule data is not persisted with owner assignments.** The NHL schedule is league-agnostic; which owner holds which team is a runtime concern. Joining draft data at persist time would create output files that are tightly coupled to a specific season and league configuration. Instead, the pipeline stores clean schedule data and the domain layer joins owner information in memory when it's needed. This keeps the pipeline reusable and avoids a file explosion if the project ever supports multiple leagues or seasons.

**Standings are computed on the fly, not cached.** The inputs (game results and draft assignments) are small enough that recomputing standings on each request is trivial. Caching computed standings would add complexity without a meaningful performance benefit at this scale.

**Parquet and CSV dual storage.** Processed data is written as both parquet (dtype-safe, used at runtime) and CSV (human-readable, useful for debugging and spot-checking). The parquet file is the source of truth; the CSV is a development convenience.

**Domain functions accept DataFrames, not file paths.** I/O belongs at the pipeline boundary. By the time data reaches the domain layer it's already loaded; domain functions receive DataFrames and return DataFrames. This keeps the separation between "getting data" and "doing something with data" clean and explicit.

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
| Data processing | pandas |
| Web framework | Flask |
| Charting | Plotly |
| Deployment | Railway |

---

## Running Locally

```bash
# Clone and install
git clone https://github.com/E-Evenson/lordstanley
cd lordstanley
poetry install

# Run the app
poetry run flask --app src/lord_stanley/web/app.py run
```

Draft data lives in `reference_data/drafts/{season}.csv` with `team_abbrev` and `owner` columns. The number of owners and teams per owner is determined entirely by this file — the app doesn't hardcode league structure. Season and initial cup holder are configured in `src/lord_stanley/config.py`.

---

## What's Next

- **Logging and error handling** — structured logging throughout the pipeline and domain layers, with graceful handling of NHL API failures
- **Tests** — unit tests for domain logic using in-memory fixtures; test data for future, live, and intermission game states is already in place
- **Pandas / Polars comparison** — a parallel Polars implementation of the pipeline to demonstrate reasoned tool selection, not just familiarity with one library
- **Documentation** — module-level docstrings are complete; inline comments for non-obvious logic remain

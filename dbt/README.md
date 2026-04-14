# Lord Stanley - dbt

dbt project for transforming raw NHL data in BigQuery.

## Models

### Staging
- `stg_schedule` — cleans and unnests raw schedule data from the NHL API

## Running dbt

```bash
dbt run
```
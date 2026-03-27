import pandas as pd

from lord_stanley.pipeline import extract, transform, load


def run_schedule_etl(season: str) -> pd.DataFrame:
    """
    Extract, transform, and load the full season schedule
    Args:
        season: The season code to run the ETL for
    """
    raw_schedule = extract.extract_season_schedule(season)
    transformed_schedule = transform.transform_season_schedule(raw_schedule)
    load.save_schedule(transformed_schedule, season)
    return transformed_schedule


if __name__ == "__main__":
    from lord_stanley.config import CURRENT_SEASON

    run_schedule_etl(CURRENT_SEASON)

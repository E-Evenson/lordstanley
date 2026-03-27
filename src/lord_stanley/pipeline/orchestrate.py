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


def run_game_etl(game_id: str) -> pd.DataFrame:
    raw_game = extract.extract_single_game(game_id)
    print(raw_game)
    transformed_game = transform.transform_game_data(raw_game)

    return transformed_game

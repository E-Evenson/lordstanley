import pandas as pd
import pytest

from lord_stanley.pipeline.transform import (
    transform_season_schedule,
    transform_game_data,
)
from lord_stanley.pipeline.constants import (
    SCHEDULE_OUTPUT_COLUMNS,
    GAME_OUTPUT_COLUMNS,
    SCHEDULE_OUTPUT_DTYPES,
    GAME_OUTPUT_DTYPES,
)


def test_transform_season_schedule_empty_input_raises_value_error():
    with pytest.raises(ValueError):
        transform_season_schedule([])


def test_winner_loser_null_for_future_game(raw_schedule):
    result = transform_season_schedule(raw_schedule)
    future_games = result[result["game_state"] == "FUT"]
    assert future_games["winner_abbrev"].isna().all()
    assert future_games["loser_abbrev"].isna().all()


def test_winner_loser_null_for_live_game(raw_schedule):
    result = transform_season_schedule(raw_schedule)
    live_games = result[result["game_state"] == "LIVE"]
    assert len(live_games) > 0, "No live games in fixture — test is inconclusive"
    assert live_games["winner_abbrev"].isna().all()
    assert live_games["loser_abbrev"].isna().all()


def test_transform_game_data_empty_input_raises_value_error():
    with pytest.raises(ValueError):
        transform_game_data({})

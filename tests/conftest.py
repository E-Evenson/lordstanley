import json
from pathlib import Path

import pytest


TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def raw_schedule():
    with open(TEST_DATA_DIR / "schedule" / "raw_schedule.json") as f:
        return json.load(f)


@pytest.fixture
def future_game():
    with open(TEST_DATA_DIR / "game" / "future_game_state.json") as f:
        return json.load(f)


@pytest.fixture
def live_game_in_reg():
    pass


@pytest.fixture
def live_game_in_int():
    pass


@pytest.fixture
def live_game_in_ot():
    pass


@pytest.fixture
def live_game_in_shootout():
    pass


@pytest.fixture
def final_game():
    pass


@pytest.fixture
def off_game():
    pass

import logging

import pandas as pd


FUTURE_GAME_COLUMNS = [
    "game_date",
    "start_time",
    "away_team_abbrev",
    "home_team_abbrev",
]

LIVE_GAME_COLUMNS = [
    "away_team_abbrev",
    "away_team_score",
    "home_team_abbrev",
    "home_team_score",
    "period_descriptor",
    "clock_time_remaining",
    "in_intermission",
]


def _map_owners(game_data: pd.DataFrame, draft: pd.DataFrame) -> pd.DataFrame:
    game_data_with_owners = game_data.copy()
    owner_map = draft.set_index("team_abbrev")["owner"]

    game_data_with_owners["home_owner"] = game_data["home_team_abbrev"].map(owner_map)
    game_data_with_owners["away_owner"] = game_data["away_team_abbrev"].map(owner_map)

    return game_data_with_owners


def format_future_game(game_data: pd.DataFrame, draft: pd.DataFrame) -> pd.DataFrame:
    future_game = game_data[FUTURE_GAME_COLUMNS]
    future_game_with_owners = _map_owners(future_game, draft)

    return future_game_with_owners


def format_live_game(game_data: pd.DataFrame, draft: pd.DataFrame) -> pd.DataFrame:
    live_game = game_data[LIVE_GAME_COLUMNS]
    live_game_with_owners = _map_owners(live_game, draft)

    if live_game_with_owners["in_intermission"].item():
        live_game_with_owners["clock_time_remaining"] = "INT"

    period = live_game_with_owners["period_descriptor"].item()
    if period == "4":
        live_game_with_owners["period_descriptor"] = "OT"
    elif period == "5":
        live_game_with_owners["period_descriptor"] = "S/O"

    return live_game_with_owners


if __name__ == "__main__":
    from lord_stanley.pipeline.orchestrate import run_game_etl

    draft = pd.read_csv("reference_data/drafts/20252026.csv")
    game_data = run_game_etl("2025021162")
    game_data_future = format_future_game(game_data, draft)
    mock_live_game = pd.DataFrame(
        {
            "away_team_abbrev": pd.array(["WPG"], dtype=pd.StringDtype()),
            "away_team_score": pd.array([2], dtype=pd.Int64Dtype()),
            "home_team_abbrev": pd.array(["COL"], dtype=pd.StringDtype()),
            "home_team_score": pd.array([2], dtype=pd.Int64Dtype()),
            "period_descriptor": pd.array(["5"], dtype=pd.StringDtype()),
            "clock_time_remaining": pd.array(["3:42"], dtype=pd.StringDtype()),
            "in_intermission": pd.array([False], dtype=pd.BooleanDtype()),
        }
    )
    game_data_live = format_live_game(mock_live_game, draft)
    print(game_data_future)
    print(game_data_live)

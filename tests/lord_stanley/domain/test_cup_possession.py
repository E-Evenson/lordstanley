import pandas as pd

from lord_stanley.domain.cup_possession import get_cup_games


def make_schedule(games: list[tuple[str, str, str | None]]) -> pd.DataFrame:
    """
    Build a minimal schedule DataFrame from (home, away, winner) tuples.

    winner of None represents an unplayed game.
    """
    return pd.DataFrame(
        {
            "home_team_abbrev": pd.array([g[0] for g in games], dtype=pd.StringDtype()),
            "away_team_abbrev": pd.array([g[1] for g in games], dtype=pd.StringDtype()),
            "winner_abbrev": pd.array([g[2] for g in games], dtype=pd.StringDtype()),
        }
    )


def test_game_involving_starting_holder_is_cup_game():
    schedule = make_schedule([("FLA", "BOS", "FLA")])
    result = get_cup_games(schedule, "FLA")
    assert len(result) == 1
    assert result["is_cup_game"].all()


def test_game_not_involving_holder_is_excluded():
    schedule = make_schedule(
        [
            ("TOR", "MTL", "TOR"),
            ("FLA", "BOS", "FLA"),
        ]
    )
    result = get_cup_games(schedule, "FLA")
    assert len(result) == 1
    assert result.iloc[0]["home_team_abbrev"] == "FLA"


def test_cup_passes_to_winner():
    schedule = make_schedule(
        [
            ("FLA", "BOS", "BOS"),  # BOS takes the cup
            ("FLA", "TOR", "TOR"),  # FLA no longer holds it — not a cup game
            ("BOS", "MTL", "MTL"),  # MTL takes the cup from BOS
        ]
    )
    result = get_cup_games(schedule, "FLA")
    assert len(result) == 2
    assert list(result["winner_abbrev"]) == ["BOS", "MTL"]


def test_holder_keeps_cup_until_loss():
    schedule = make_schedule(
        [
            ("FLA", "BOS", "FLA"),
            ("TOR", "FLA", "FLA"),
            ("FLA", "MTL", "MTL"),
        ]
    )
    result = get_cup_games(schedule, "FLA")
    assert len(result) == 3
    assert list(result["winner_abbrev"]) == ["FLA", "FLA", "MTL"]


def test_next_unplayed_cup_game_is_included():
    schedule = make_schedule(
        [
            ("FLA", "BOS", "FLA"),
            ("FLA", "TOR", None),  # next cup game, not yet played
        ]
    )
    result = get_cup_games(schedule, "FLA")
    assert len(result) == 2
    assert pd.isna(result.iloc[-1]["winner_abbrev"])


def test_games_after_unplayed_cup_game_are_excluded():
    schedule = make_schedule(
        [
            ("FLA", "BOS", None),  # next cup game — iteration stops here
            ("FLA", "TOR", "FLA"),  # involves holder but must not be marked
        ]
    )
    result = get_cup_games(schedule, "FLA")
    assert len(result) == 1
    assert pd.isna(result.iloc[0]["winner_abbrev"])


def test_possession_follows_schedule_order():
    # If order were ignored, the FLA/TOR game would look like a cup game.
    schedule = make_schedule(
        [
            ("FLA", "BOS", "BOS"),  # cup moves to BOS first
            ("FLA", "TOR", "TOR"),  # FLA game after losing the cup
            ("BOS", "TOR", "BOS"),
        ]
    )
    result = get_cup_games(schedule, "FLA")
    assert list(result["winner_abbrev"]) == ["BOS", "BOS"]


def test_input_schedule_is_not_mutated():
    schedule = make_schedule([("FLA", "BOS", "FLA")])
    original = schedule.copy()
    get_cup_games(schedule, "FLA")
    pd.testing.assert_frame_equal(schedule, original)


def test_no_cup_games_returns_empty():
    schedule = make_schedule([("TOR", "MTL", "TOR")])
    result = get_cup_games(schedule, "FLA")
    assert result.empty

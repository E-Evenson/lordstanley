import logging
from pathlib import Path

import pandas as pd

import read_config
import nhl_api


logger = logging.getLogger(__name__)

logger.info(f"imported {__name__}")


def _extract_schedule() -> pd.DataFrame:
    season = int(config["seasons"]["season"])
    teams = list(pd.read_csv(Path.cwd() / "static/teams.csv")["triCode"])

    raw_schedule_df = nhl_api.get_full_schedule(season, teams)

    return raw_schedule_df


def _add_winners_and_losers_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["winnerId"] = df.apply(
        lambda x: x["homeTeam.id"]
        if x["homeTeam.score"] > x["awayTeam.score"]
        else x["awayTeam.id"]
        if x["homeTeam.score"] < x["awayTeam.score"]
        else 0,
        axis=1,
    )

    df["loserId"] = df.apply(
        lambda x: 0
        if x.winnerId == 0
        else x["homeTeam.id"]
        if x.winnerId == x["awayTeam.id"]
        else x["awayTeam.id"],
        axis=1,
    )

    return df


def _identify_cup_games(
    schedule_df: pd.DataFrame, lord_stanley: str
) -> pd.DataFrame:
    schedule_df[["cupGame", "nextGame"]] = False
    for i, game in schedule_df.iterrows():
        # determine if game is a cup game
        if (
            lord_stanley == game["homeTeam.id"]
            or lord_stanley == game["awayTeam.id"]
        ):
            schedule_df.loc[i, "cupGame"] = True

            # determine whether game has been played
            if game["gameState"] != "OFF" and game["gameState"] != "FINAL":
                schedule_df.loc[i, "nextGame"] = True
                break

            lord_stanley = game.at["winnerId"]
        else:
            schedule_df.loc[i, "cupGame"] = False

    return schedule_df


def _find_starting_lord_stanley():
    pass


def _transform_schedule(raw_schedule_df: pd.DataFrame) -> pd.DataFrame:
    column_dtypes = {
        "id": "int",
        "season": "str",
        "gameType": "int",
        "gameDate": "datetime64[ns]",
        "startTimeUTC": "datetime64[ns, UTC]",
        "gameState": "str",
        "awayTeam.id": "int",
        "awayTeam.placeName.default": "str",
        "awayTeam.abbrev": "str",
        "awayTeam.logo": "str",
        "awayTeam.score": "int",
        "homeTeam.id": "int",
        "homeTeam.placeName.default": "str",
        "homeTeam.abbrev": "str",
        "homeTeam.logo": "str",
        "homeTeam.score": "int",
    }

    cleaned_schedule = raw_schedule_df[column_dtypes.keys()]
    cleaned_schedule = cleaned_schedule.fillna(0)
    cleaned_schedule = cleaned_schedule.astype(column_dtypes)

    # regular season games only
    cleaned_schedule = cleaned_schedule[
        cleaned_schedule["gameType"] == 2
    ].reset_index(drop=True)

    return cleaned_schedule


def validate_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    possible issues:
    missing column - key error
    extra column - no error
    column has wrong dtype - value error
    """
    column_dtypes = {
        "id": int,
        "season": str,
        "gameType": int,
        "gameDate": "datetime64[ns]",
        "startTimeUTC": "datetime64[ns, UTC]",
        "gameState": str,
        "awayTeam.id": int,
        "awayTeam.placeName.default": str,
        "awayTeam.abbrev": str,
        "awayTeam.logo": str,
        "awayTeam.score": int,
        "homeTeam.id": int,
        "homeTeam.placeName.default": str,
        "homeTeam.abbrev": str,
        "homeTeam.logo": str,
        "homeTeam.score": int,
        "winnerId": int,
        "loserId": int,
        "cupGame": bool,
        "nextGame": bool,
    }

    df = df.astype(column_dtypes)

    return df


def _load_schedule(df: pd.DataFrame, filepath: Path):
    validate_dtypes(df)
    df.to_csv(filepath)
    return


def full_schedule_refresh(season, lord_stanley):
    raw_schedule = _extract_schedule()
    cleaned_schedule = _transform_schedule(raw_schedule)
    cleaned_schedule = _add_winners_and_losers_columns(cleaned_schedule)
    cleaned_schedule = _identify_cup_games(cleaned_schedule, lord_stanley)

    cleaned_schedule.to_csv(Path.cwd() / f"static/{season}_schedule.csv")

    return cleaned_schedule


def read_schedule(season: str) -> pd.DataFrame:
    schedule = pd.read_csv(Path.cwd() / f"./static/{season}_schedule.csv", index_col=0)
    schedule = validate_dtypes(schedule)
    schedule = schedule[schedule["season"] == season]

    return schedule


if __name__ == "__main__":
    logging.basicConfig(
        level="DEBUG",
        filename=Path.cwd() / "logs/nhl_api.log",
        filemode="w",
        format="%(asctime)s: %(name)s: %(levelname)s: %(message)s",
    )

    config = read_config.main()

    logger.info("testing shit")

    test_season = "20242025"
    test_lord_stanley = 13

    nhl_schedule = full_schedule_refresh(test_season, test_lord_stanley)
    nhl_schedule.to_csv(Path.cwd() / "data/interim/transformed_schedule.csv")

    static_schedule = read_schedule(test_season)

    if nhl_schedule.equals(static_schedule):
        print("etl and saved schedules match")

from pathlib import Path

import pandas as pd

import read_config
from schedule_etl import read_schedule

config = read_config.main()


def _get_draft_details(season: str, league: str) -> pd.DataFrame:
    draft = pd.read_csv(config["drafts"]["path"])
    draft_dtypes = {"triCode": str, "owner": str, "season": str, "league": str}
    draft = draft.astype(draft_dtypes)

    draft = draft[(draft["season"] == season) & (draft["league"] == league)]

    return draft


def _get_schedule_details(season: str):
    schedule = pd.read_csv(Path.cwd() / "./static/20242025_schedule.csv")

    return schedule


def _calculate_points(season: str, league: str) -> pd.Series:
    cup_games = read_schedule(season)
    cup_games = cup_games[~cup_games["nextGame"]]
    cup_games = cup_games[cup_games["cupGame"]][["winnerId", "loserId"]]
    print(cup_games)
    points = cup_games["winnerId"]
    points = points.value_counts()

    return points


def calculate_games_played():
    return


def main(season: str, league: str):
    return


if __name__ == "__main__":
    _calculate_points("20242025", "lord_stanley")

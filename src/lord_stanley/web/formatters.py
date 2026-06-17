"""
Formatting layer for web display

Responsibilities:
    - Formatting dataframes for display
    - Determining next game display state
    - Creating cumulative points chart
    - Converting to HTML
"""

import logging

import pandas as pd
import plotly.express as px  # type: ignore[import-untyped]


logger = logging.getLogger(__name__)


FUTURE_GAME_COLUMNS = {
    "game_date": "Date",
    "start_time": "Time",
    "away_team_abbrev": "Away Team",
    "away_owner": "Away Owner",
    "home_team_abbrev": "Home Team",
    "home_owner": "Home Owner",
}


LIVE_GAME_COLUMNS = {
    "away_team_abbrev": "Away",
    "away_owner": "Away Owner",
    "away_team_score": "Away Score",
    "home_team_abbrev": "Home",
    "home_owner": "Home Owner",
    "home_team_score": "Home Score",
    "period_descriptor": "Period",
    "clock_time_remaining": "Time Remaining",
}


TEAM_STATS_COLUMNS = {
    "position": "Rank",
    "team_abbrev": "Team",
    "points": "Points",
    "games_played": "Games Played",
}


def format_league_standings(league_standings: pd.DataFrame) -> str:
    """
    Formats league standings and adds win % column

    Args:
        league_standings: unformatted league stats and standings

    Returns:
        HTML for league standings table
    """

    logger.debug("Formatting league standings table.")

    formatted_league_standings = league_standings.copy()
    formatted_league_standings["win_percentage"] = formatted_league_standings[
        "win_percentage"
    ].apply(lambda x: f"{x}%")
    formatted_league_standings = formatted_league_standings.rename(
        columns={
            "position": "Position",
            "owner": "Owner",
            "points": "Points",
            "games_played": "Games Played",
            "win_percentage": "Win %",
        }
    )

    standings_html = pd.DataFrame.to_html(formatted_league_standings, index=False)

    return standings_html


def _map_owners(game_data: pd.DataFrame, draft: pd.DataFrame) -> pd.DataFrame:
    """
    Map owners to game data

    Args:
        game_data: next game data
        draft: draft data

    Returns:
        Dataframe with next game data with owners added, formatted for display
    """
    game_data_with_owners = game_data.copy()
    owner_map = draft.set_index("team_abbrev")["owner"]

    game_data_with_owners["home_owner"] = game_data["home_team_abbrev"].map(owner_map)
    game_data_with_owners["away_owner"] = game_data["away_team_abbrev"].map(owner_map)

    return game_data_with_owners


def _format_future_game(game_data: pd.DataFrame) -> pd.DataFrame:
    """
    Formats next game data for future games

    Args:
        game_data: next game data

    Returns:
        Dataframe with next game data formatted for future games
    """
    future_game = game_data.copy()
    future_game["game_date"] = future_game["game_date"].dt.strftime("%B %e, %Y")
    future_game["start_time"] = (
        future_game["start_time"]
        .dt.tz_convert("America/Edmonton")
        .dt.strftime("%H:%M")
        .astype(pd.StringDtype())
    )

    future_game = future_game[FUTURE_GAME_COLUMNS.keys()].rename(
        columns=FUTURE_GAME_COLUMNS
    )
    return future_game


def _format_live_game(game_data: pd.DataFrame) -> pd.DataFrame:
    """
    Formats next game data for live games

    Args:
        game_data: next game data

    Returns:
        Dataframe with next game data formatted for live games
    """
    live_game = game_data.copy()

    if live_game["in_intermission"].item():
        live_game["clock_time_remaining"] = "INT"

    period = live_game["period_descriptor"].item()
    if period == "4":
        live_game["period_descriptor"] = "OT"
    elif period == "5":
        live_game["period_descriptor"] = "S/O"

    live_game = live_game[LIVE_GAME_COLUMNS.keys()].rename(columns=LIVE_GAME_COLUMNS)

    return live_game


def format_next_game(
    next_game_raw: pd.DataFrame, next_game_state: str, draft: pd.DataFrame
) -> str:
    """
    Formats next game data for display

    Args:
        next_game_raw: unformatted next game data
        next_game_state: state of next game
        draft: draft data

    Returns:
        HTML for next game table
    """

    logger.debug("Formatting next game table.")

    formatted_next_game = next_game_raw.copy()
    formatted_next_game = _map_owners(formatted_next_game, draft)

    if next_game_state in [
        "FUT",
        "PRE",
    ]:
        formatted_next_game = _format_future_game(formatted_next_game)
    elif next_game_state in [
        "LIVE",
        "CRIT",
    ]:
        formatted_next_game = _format_live_game(formatted_next_game)

    next_game_html = pd.DataFrame.to_html(formatted_next_game, index=False)

    return next_game_html


def format_cumulative_points_chart(
    cumulative_owner_stats_raw: pd.DataFrame,
) -> str:
    """
    Creates chart of cumulative owner stats

    Args:
        cumulative_owner_stats_raw: unformatted cumulative owner stats

    Returns:
        HTML for cumulative points chart
    """

    logger.debug("Formatting cumulative points chart.")

    cumulative_owner_stats = cumulative_owner_stats_raw.copy()
    cumulative_points_chart = px.line(
        cumulative_owner_stats,
        x="game_date",
        y="cumulative_points",
        color="owner",
        title="Points Over Time",
        labels={
            "game_date": "Date",
            "cumulative_points": "Points",
            "owner": "Owner",
        },
    )

    cumulative_points_chart = cumulative_points_chart.to_html(
        full_html=False, include_plotlyjs="cdn"
    )

    return cumulative_points_chart


def format_team_stats(raw_team_stats: pd.DataFrame) -> dict[str, str]:
    """
    Formats teams stats for each owner for display

    Args:
        raw_team_stats: unformatted next game data

    Returns:
        Dict of owners with values being HTML of the stats tables of the teams they own
    """
    logger.debug("Formatting team stats per owner.")

    all_team_stats = raw_team_stats.copy()

    team_stats = {}

    owners = all_team_stats["owner"].unique()
    for owner in owners:
        owner_team_stats = all_team_stats[all_team_stats["owner"] == owner]
        owner_team_stats = owner_team_stats[TEAM_STATS_COLUMNS.keys()].rename(
            columns=TEAM_STATS_COLUMNS
        )
        team_stats[owner] = owner_team_stats

    team_stats = dict(sorted(team_stats.items()))

    for owner in team_stats:
        team_stats[owner] = pd.DataFrame.to_html(team_stats[owner], index=False)

    return team_stats

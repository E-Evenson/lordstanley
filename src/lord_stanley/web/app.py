import logging

from flask import Flask, render_template
import pandas as pd
import plotly.express as px  # type: ignore[import-untyped]

from lord_stanley import orchestrate

logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route("/")
def run():

    display_data = orchestrate.run()

    league_standings = (
        display_data["league_standings"]
        .reset_index()
        .rename(
            columns={
                "position": "Position",
                "owner": "Owner",
                "points": "Points",
                "games_played": "Games Played",
            }
        )
    )
    league_standings_html = pd.DataFrame.to_html(league_standings, index=False)

    next_game_state = display_data["next_game_state"]
    next_game = display_data["next_game"]
    next_game["game_date"] = next_game["game_date"].dt.strftime("%B %d, %Y")
    next_game = next_game[
        [
            "game_date",
            "start_time",
            "away_team_abbrev",
            "away_owner",
            "home_team_abbrev",
            "home_owner",
        ]
    ].rename(
        columns={
            "game_date": "Date",
            "start_time": "Start Time",
            "away_team_abbrev": "Away Team",
            "away_owner": "Away Owner",
            "home_team_abbrev": "Home Team",
            "home_owner": "Home Owner",
        }
    )
    is_live = ""
    if next_game_state in ["OFF", "FINAL"]:
        next_game_html = (
            f"No more games. Ya boi {league_standings['Owner'].values[0]} wins!"
        )
    elif next_game_state in ["LIVE", "CRIT"]:
        is_live = " (LIVE)"
        next_game_html = pd.DataFrame.to_html(next_game, index=False)
    else:
        next_game_html = pd.DataFrame.to_html(next_game, index=False)

    cumulative_owner_stats = display_data["cumulative_owner_stats"]
    fig = px.line(
        cumulative_owner_stats,
        x="game_date",
        y="owner_cumulative_wins",
        color="owner",
        title="Points Over Time",
    )
    chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

    return render_template(
        "index.html",
        standings_table=league_standings_html,
        next_game_table=next_game_html,
        is_live=is_live,
        chart=chart_html,
    )


if __name__ == "__main__":
    app.run(debug=True)

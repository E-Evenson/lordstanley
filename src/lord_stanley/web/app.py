"""
Lord Stanley webapp

Responsibilities:
    - Calling domain layer and passing league_id
    - Passing raw data to formatter layer
    - Next game display formatting
"""

import logging

from flask import Flask, abort, redirect, render_template, url_for

from lord_stanley.config import PIPELINE_RUN_METHOD
from lord_stanley.domain import orchestrate as domain_orchestrator
from lord_stanley.web import formatters
from lord_stanley.logging_config import configure_logging

logger = logging.getLogger(__name__)
configure_logging()

app = Flask(__name__)


@app.route("/")
def home():
    """
    Redirect the bare root URL to the default league page
    """
    return redirect(url_for("index", league_id="lord-stanley"))


@app.route("/<league_id>/")
def index(league_id: str):
    """
    Render the Lord Stanley league standings and next cup game page.

    Args:
        league_id: id of the league to display, taken from the url
    """
    logger.info(
        f"Rendering league {league_id}. Pipeline run method: {PIPELINE_RUN_METHOD}"
    )

    try:
        if PIPELINE_RUN_METHOD == "scheduled":
            display_data = domain_orchestrator.run_league_calculations_sql(
                league_id=league_id
            )
        else:
            display_data = domain_orchestrator.run_live_league_calculations(
                league_id=league_id
            )
    except domain_orchestrator.LeagueNotFoundError:
        abort(404)

    raw_league_standings = display_data["league_standings"]
    league_standings_html = formatters.format_league_standings(raw_league_standings)

    next_game_state = display_data["next_game_state"]
    is_live = next_game_state in ["LIVE", "CRIT"]
    if next_game_state in [
        "FINAL",
        "OFF",
    ]:
        next_game_html = (
            f"Season complete. {raw_league_standings['owner'].iloc[0]} wins!"
        )
    else:
        draft = display_data["draft"]
        raw_next_game = display_data["next_game"]
        next_game_html = formatters.format_next_game(
            raw_next_game, next_game_state, draft
        )

    cumulative_owner_stats = display_data["cumulative_owner_stats"]
    cumulative_points_chart_html = formatters.format_cumulative_points_chart(
        cumulative_owner_stats
    )

    raw_team_stats = display_data["team_stats"]
    team_stats_html = formatters.format_team_stats(raw_team_stats)

    return render_template(
        "index.html",
        league_id=league_id,
        standings_table=league_standings_html,
        next_game_table=next_game_html,
        is_live=is_live,
        chart=cumulative_points_chart_html,
        team_stats=team_stats_html,
    )


if __name__ == "__main__":
    app.run(debug=True)

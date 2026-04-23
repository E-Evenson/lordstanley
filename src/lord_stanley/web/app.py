"""
Lord Stanley webapp

Responsibilities:
    - Calling domain layer
    - Passing raw data to formatter layer
    - Next game display formatting
"""

import logging

from flask import Flask, render_template

from lord_stanley.config import PIPELINE_RUN_METHOD
from lord_stanley.domain import orchestrate as domain_orchestrator
from lord_stanley.web import formatters

logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route("/")
def index():
    """
    Render the Lord Stanley league standings and next cup game page.
    """
    if PIPELINE_RUN_METHOD == "scheduled":
        display_data = domain_orchestrator.run_league_calculations_sql()
    else:
        display_data = domain_orchestrator.run_live_league_calculations()

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
        standings_table=league_standings_html,
        next_game_table=next_game_html,
        is_live=is_live,
        chart=cumulative_points_chart_html,
        team_stats=team_stats_html,
    )


if __name__ == "__main__":
    app.run(debug=True)

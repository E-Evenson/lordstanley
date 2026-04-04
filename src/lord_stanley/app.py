"""
Lord Stanley webapp

Responsibilities:
    - Calling domain layer
    - Passing raw data to formatter layer
    - Next game display formatting
    - Converting to html
"""

import logging

from flask import Flask, render_template
import pandas as pd

from lord_stanley.domain import orchestrate as domain_orchestrator
from lord_stanley.web import formatters

logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route("/")
def index():
    """
    Render the Lord Stanley league standings and next cup game page.
    """
    display_data = domain_orchestrator.run_league_calculations()

    raw_league_standings = display_data["league_standings"]
    league_standings = formatters.format_league_standings(raw_league_standings)
    league_standings_html = pd.DataFrame.to_html(league_standings, index=False)

    next_game_state = display_data["next_game_state"]
    is_live = ""
    if next_game_state in [
        "FINAL",
        "OFF",
    ]:
        next_game_html = f"Season complete. {league_standings['Owner'].iloc[0]} wins!"
    else:
        draft = display_data["draft"]
        raw_next_game = display_data["next_game"]
        next_game, is_live = formatters.format_next_game(
            raw_next_game, next_game_state, draft
        )
        next_game_html = pd.DataFrame.to_html(next_game, index=False)

    cumulative_owner_stats = display_data["cumulative_owner_stats"]
    chart = formatters.format_cumulative_points_chart(cumulative_owner_stats)
    chart_html = chart.to_html(full_html=False, include_plotlyjs="cdn")

    return render_template(
        "index.html",
        standings_table=league_standings_html,
        next_game_table=next_game_html,
        is_live=is_live,
        chart=chart_html,
    )


if __name__ == "__main__":
    app.run(debug=True)

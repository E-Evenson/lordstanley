from flask import Flask, render_template
import pandas as pd
import src.calculator as calc
from pathlib import Path
import src.schedule_calculations as schedule_etl
import src.next_game as next
import src.transform as transform


def read_schedule():

    schedule_filepath = Path.cwd()/'data/season_schedule.csv'

    try:
        schedule = pd.read_csv(schedule_filepath)
    except:
        schedule = schedule_etl.main()
        schedule = transform.main()

    return schedule

def get_next_game(schedule):

    next_game_data = next.main(schedule)

    return next_game_data


app = Flask("__name__")
@app.route('/')
def standings():

    schedule = read_schedule()

    is_live = ''

    standings_path = Path.cwd()/'data/standings.csv'
    owner_standings = pd.read_csv(standings_path, index_col=0)

    standings_table = pd.DataFrame.to_html(owner_standings)

    
    next_game = get_next_game(schedule)
    if next_game.empty:
        next_game_table = f"No more games. Ya boi {owner_standings['Owner'].values[0]} wins!"
    elif 'Period' in next_game.columns:
        is_live = " (LIVE)"
        next_game_table = pd.DataFrame.to_html(next_game, index=False)
    else:
        next_game_table = pd.DataFrame.to_html(next_game, index=False)

    # owner_standings = calc.main()

    # next_game_table = pd.DataFrame.to_html(next_game, index=False)

    # graph_path = Path.cwd()/'static/my_plot.png'
        # <img src="./static/my_plot.png" height="480px" width='600px'>


    return f"""
    <h1>Standings:</h1>
    <p>{standings_table}</p>
    <br><br><h1>Next Game{is_live}:</h1>
    <p>{next_game_table}</p>
    <img src="./static/my_plot.png" height="480px" width='600px'>
    """

if __name__ == "__main__":
    app.run(debug=True)
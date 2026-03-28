import pandas as pd
from pathlib import Path
import requests

if __name__ != "__main__":
    import scripts.calculator as calc
    import scripts.full_schedule_etl as schedule_etl
    import scripts.transform as transform

def _get_single_game_data(game_id):

    game_url = f'https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore'

    response = requests.get(game_url)

    game_data = response.json()

    game_data = pd.json_normalize(game_data)

    game_data.to_csv(Path.cwd()/f'data/learning/{game_id}.csv')

    return game_data

def _future_game(raw_game_data):

    useful_game_data = raw_game_data[['gameDate', 'awayTeam.abbrev', 'Away Owner',
                                        'homeTeam.abbrev', 'Home Owner']].rename(
                                            columns={'gameDate': 'Date', 'awayTeam.abbrev': 'Away',
                                                    'homeTeam.abbrev': 'Home'}
                                        )

    useful_game_data['Date'] = pd.to_datetime(useful_game_data['Date'])
    useful_game_data['Date'] = useful_game_data['Date'].dt.strftime('%B %d, %Y')

    return useful_game_data

def _live_game(raw_game_data):

    useful_game_data = raw_game_data[['periodDescriptor.number', 'clock.timeRemaining',
                                       'awayTeam.abbrev', 'Away Owner', 'awayTeam.score',
                                         'homeTeam.abbrev', 'Home Owner', 'homeTeam.score'
                                         ]].rename(columns={'periodDescriptor.number': "Period", 'clock.timeRemaining': "Time Remaining",
                                                                'awayTeam.abbrev': "Away", 'awayTeam.score': "Away Score",
                                                                'homeTeam.abbrev': "Home", 'homeTeam.score': "Home Score"})

    
    if raw_game_data['clock.inIntermission'].iloc[0] == True:
        useful_game_data['Time Remaining'] = "Intermission"

    if useful_game_data['Period'].iloc[0] == 4:
        useful_game_data['Period'] = "Overtime"
    elif useful_game_data['Period'].iloc[0] == 5:
        useful_game_data['Period'] = "Shootout"

    return useful_game_data

def _finished_game(season='20232024', draft_season='20232024'):

    schedule_etl.main(season, draft_season)
    schedule = transform.main(season, draft_season)
    calc.main()

    useful_game_data = main(schedule)

    return useful_game_data

def main(schedule):

    next_game = schedule[schedule['is_next_game']]
    print(next_game)
    if next_game.empty:
        next_game_details = next_game
        return next_game_details
    
    next_game_id = next_game['id'].iloc[0]
    home_owner = next_game['home_owner'].iloc[0]
    away_owner = next_game['away_owner'].iloc[0]

    raw_game_data = _get_single_game_data(next_game_id)
    raw_game_data['Home Owner'] = home_owner
    raw_game_data['Away Owner'] = away_owner
    game_state = raw_game_data['gameState'].iloc[0]

    if game_state == 'FUT' or game_state == 'PRE':

        next_game_details = _future_game(raw_game_data)

    elif game_state == "LIVE" or game_state == "CRIT":

        next_game_details = _live_game(raw_game_data)

        next_game_details[['Home Owner', 'Away Owner']] = [home_owner, away_owner]

    elif game_state == "OFF" or game_state == "FINAL":

        next_game_details = _finished_game()


    return next_game_details

if __name__ == '__main__':

    import calculator as calc
    import src.schedule_calculations as schedule_etl
    import transform as transform


    schedule_filepath = Path.cwd()/'data/season_schedule.csv'
    schedule = pd.read_csv(schedule_filepath)

    print(main(schedule))
import pandas as pd
from pathlib import Path

import nhl_api


def _clean_schedule(raw_schedule_df: pd.DataFrame) -> pd.DataFrame:

    cleaned_schedule = raw_schedule_df.drop_duplicates(subset='id')

    cleaned_schedule = cleaned_schedule[['id', 
                                         'gameType', 
                                         'gameDate', 
                                         'gameState', 
                                         'awayTeam.id', 
                                         'awayTeam.abbrev',
                                         'awayTeam.score', 
                                         'homeTeam.id', 
                                         'homeTeam.abbrev', 
                                         'homeTeam.score',
                                         ]]

    cleaned_schedule = cleaned_schedule.sort_values('id').reset_index()
    cleaned_schedule = cleaned_schedule[cleaned_schedule['gameType'] == 2]

    cleaned_schedule = cleaned_schedule.assign(winner_id = 0, loser_id = 0, is_cup_game = False, is_next_game = False)

    cleaned_schedule['winner_id'] = cleaned_schedule.apply(lambda x: 0 if (x.gameState == 'LIVE' or x.gameState == "FUT" 
                                                                 or x.gameState == "PRE" or x.gameState == "CRIT")
                                                 else x['homeTeam.id'] if x['homeTeam.score']>x['awayTeam.score'] 
                                                 else x['awayTeam.id'], axis=1)
    
    cleaned_schedule['loser_id'] = cleaned_schedule.apply(lambda x: 0 if x.winner_id == 0
                                    else x['homeTeam.id'] if x.winner_id == x['awayTeam.id']
                                    else x['awayTeam.id'], axis=1)

    return cleaned_schedule


def refresh_schedule():

    raw_schedule = nhl_api.get_full_season_schedule()
    cleaned_schedule = _clean_schedule(raw_schedule)
    print(raw_schedule)
    pass

def main(nhl_season='20242025', draft_season='20242025'):


    return

if __name__ == "__main__":

    refresh_schedule()






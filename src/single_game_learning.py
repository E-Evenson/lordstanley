import requests
import pandas as pd
from pathlib import Path

def _get_single_game_data(game_id):

    # game_url = f'https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore'

    # response = requests.get(game_url)

    # game_data = response.json()

    # game_data = pd.json_normalize(game_data)

    game_data = pd.read_json(Path.cwd()/'data/learning/overtime json.json', orient='records')

    game_data = pd.json_normalize(game_data)

    print(game_data)

    return game_data

def main(game_id):
    save_me = _get_single_game_data(game_id)
    # save_path = Path.cwd()/f'data/learning/{game_id}.csv'
    # save_me.to_csv(save_path)
    return

if __name__ == "__main__":

    game_id = '2023021019'
    main(game_id)
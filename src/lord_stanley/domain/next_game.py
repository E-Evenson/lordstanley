import logging

import pandas as pd

from nhl_api.api import fetch_game_data

result = fetch_game_data("2025021144")
result = pd.json_normalize(result)
print(result.info())

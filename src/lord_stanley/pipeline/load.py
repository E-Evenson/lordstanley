import pandas as pd

from lord_stanley.config import PROCESSED_DIR


def save_schedule(df: pd.DataFrame, season: str) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_DIR / f"{season}_schedule.csv", index=False)

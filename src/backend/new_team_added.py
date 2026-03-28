from pathlib import Path

import nhl_api


def main():
    all_teams = nhl_api.refresh_team_data()
    all_teams.to_csv(Path.cwd() / "static/teams.csv")

    return


if __name__ == "__main__":
    main()

import pandas as pd
from pathlib import Path
import datetime as dt
import seaborn as sns
import matplotlib.pyplot as plt

plt.switch_backend('Agg') 


def calculate_team_stats(teams, schedule):

    win_counts = schedule.winner_id.value_counts(dropna=False)
    wins_added = pd.merge(teams, win_counts, how='left', left_on='id', 
                          right_on='winner_id').rename(columns={'count':'points'})

    home_games = schedule['homeTeam.id'].value_counts(dropna=False)
    home_games_added = pd.merge(wins_added, home_games, how='left', left_on='id', 
                                right_on='homeTeam.id').rename(columns={'count':'home_games'})

    away_games = schedule['awayTeam.id'].value_counts(dropna=False)
    away_games_added = pd.merge(home_games_added, away_games, how='left', left_on='id', 
                                right_on='awayTeam.id').rename(columns={'count':'away_games'}).fillna(0)

    games_played_added = away_games_added.assign(games_played = away_games_added.home_games + away_games_added.away_games)

    team_stats = games_played_added
    team_stats[['points', 'games_played']] = team_stats[['points', 'games_played']].astype('int')

    return team_stats

def calculate_owner_standings(schedule, owners_filepath):

    owner_stats = pd.read_csv(owners_filepath)

    played_games = pd.concat([schedule[schedule['gameState']=='OFF'], schedule[schedule['gameState']=='FINAL']])

    win_counts = played_games.winner_owner.value_counts()

    games_played = played_games.home_owner.value_counts() + played_games.away_owner.value_counts()

    owner_stats = pd.merge(owner_stats, win_counts, left_on='owner', right_index=True).rename(columns={'count':'Points'})
    owner_stats = pd.merge(owner_stats, games_played, left_on='owner', right_index=True).rename(columns={'count':'Games Played'})

    owner_stats = owner_stats.rename(columns={'owner':'Owner'})

    owner_standings = owner_stats.sort_values(['Points', 'Games Played'], ascending=[False, True]).reset_index(drop=True)
    owner_standings.index += 1

    return owner_standings

def create_points_graph(cup_games):

    finished_cup_games = cup_games[cup_games['gameState'] != 'LIVE']
    finished_cup_games = finished_cup_games[finished_cup_games['gameState'] != 'FUT']

    finished_cup_games['winner_cumulative_points'] = finished_cup_games[['winner_owner', 'is_cup_game']].groupby('winner_owner').cumcount() + 1
    finished_cup_games['winner_cumulative_points'] = finished_cup_games['winner_cumulative_points'].fillna('0').astype(int)
    # print(finished_cup_games)

    owner_points_raw = finished_cup_games[['gameDate', 'winner_owner', 'winner_cumulative_points']]

    max_points = owner_points_raw['winner_cumulative_points'].max() + 1

    # if max_points % 2 == 1:
    #     max_points += 3
    # else:max_points+=2

    owner_points_line = owner_points_raw

    def owner_points(owner):
        # print(owner)

        owner_points_by_date = owner_points_raw[owner_points_raw['winner_owner'] == owner]
        owner_points_by_date = owner_points_by_date.rename(columns={'winner_cumulative_points':owner})

        return owner_points_by_date
    
    owners = ['Ani', 'Eric', 'Madhav', 'Rob']

    for owner in owners:
        # print(owner)
        to_merge = owner_points(owner)[['gameDate', owner]]
        # print(to_merge)
        owner_points_line = pd.merge(owner_points_line, to_merge, how='left', on=['gameDate']).ffill().fillna('0')
        owner_points_line[owner] = owner_points_line[owner].astype(int)
        # print(owner_points_line)
    
    # print(owner_points_line)

    plot_data = owner_points_line[['gameDate', 'Ani', 'Eric', 'Madhav', 'Rob']]
    plot_data = pd.melt(plot_data, id_vars='gameDate', value_vars=owners)
    # print(plot_data)

    # eric_points = owner_points_line[owner_points_line['winner_owner'] == 'Eric']
    # print(eric_points)

    # testing_stuff = owner_points_line
    # testing_stuff['eric_points'] = testing_stuff['winner_cumulative_points']testing_stuff['winner_owner']]

    # sns.lineplot(data=plot_data[owners])
    my_plot = sns.lineplot(x='gameDate', y='value', hue='variable', data=plot_data)
    my_plot.set_xticklabels(my_plot.get_xticklabels(), rotation=90, size=6)
    my_plot.set_ylabel('Points')
    my_plot.set_ybound(-1, max_points)
    my_plot.set_yticks(range(0, max_points, 2))
    my_plot.legend(title='Owner')
    fig = my_plot.get_figure()
    fig.savefig('./static/my_plot.png')
    plt.close()
    # plt.show()

    return

def main():

    schedule_filepath = Path.cwd()/'data/season_schedule.csv'
    schedule = pd.read_csv(schedule_filepath)

    owners_filepath = Path.cwd()/'data/owners/owners.csv'

    cup_games = schedule[schedule['is_cup_game'] == True]

    standings = calculate_owner_standings(cup_games, owners_filepath)

    standings_filepath = Path.cwd()/'data/standings.csv'
    standings.to_csv(standings_filepath)

    create_points_graph(cup_games)

    return standings

if __name__ == '__main__':

    print(main())

    # next_game_details(None)


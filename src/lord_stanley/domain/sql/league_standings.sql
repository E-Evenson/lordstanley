SELECT position, owner, points, games_played, win_percentage
FROM `lord-stanley.mart.fct_league_standings`
WHERE season = {season}
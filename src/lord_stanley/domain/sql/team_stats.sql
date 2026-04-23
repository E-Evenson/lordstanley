SELECT position, owner, team_abbrev, points, games_played
FROM `lord-stanley.mart.fct_team_stats`
WHERE season = '{season}'
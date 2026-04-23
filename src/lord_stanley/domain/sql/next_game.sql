SELECT game_date, game_state, start_time, away_team_abbrev, home_team_abbrev
FROM lord-stanley.intermediate.int_cup_possession
WHERE season = {season}
ORDER BY id DESC
LIMIT 1
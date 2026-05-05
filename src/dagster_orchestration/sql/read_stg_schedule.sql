SELECT season, id, game_type, game_date, start_time, game_state, away_team_abbrev, away_team_score, home_team_abbrev, home_team_score, winner_abbrev, loser_abbrev
FROM `lord-stanley.staging.stg_schedule`
ORDER BY id ASC
SELECT game_date, owner, owner_cumulative_wins
FROM `lord-stanley.mart.fct_cumulative_points`
WHERE season = '{season}'
{{ config(materialized='ephemeral') }}
SELECT season, id, game_type, game_date, game_state, away_team_abbrev, away_team_score, home_team_abbrev, home_team_score, is_cup_game, winner_abbrev AS team_abbrev, winner_owner AS owner, 1 AS is_win
FROM {{ ref('int_cup_possession_with_owners') }}
WHERE winner_owner IS NOT NULL
UNION ALL
SELECT season, id, game_type, game_date, game_state, away_team_abbrev, away_team_score, home_team_abbrev, home_team_score, is_cup_game, loser_abbrev AS team_abbrev, loser_owner AS owner, 0 AS is_win
FROM {{ ref('int_cup_possession_with_owners') }}
WHERE loser_owner IS NOT NULL
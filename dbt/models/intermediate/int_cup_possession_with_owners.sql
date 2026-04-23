{{ config(materialized='ephemeral') }}
WITH draft AS (
    SELECT team_abbrev, owner, season
    FROM {{ ref('drafts') }}
),
cup_games AS (
    SELECT season, id, game_type, game_date, game_state, away_team_abbrev, away_team_score, home_team_abbrev, home_team_score, winner_abbrev, loser_abbrev, is_cup_game
    FROM {{ source('intermediate', 'int_cup_possession') }}
)
SELECT cup_games.season AS season, id, game_type, game_date, game_state, away_team_abbrev, away_team_score, home_team_abbrev, home_team_score, winner_abbrev, loser_abbrev, is_cup_game, winner_draft.owner AS winner_owner, loser_draft.owner AS loser_owner
FROM cup_games
LEFT JOIN draft AS winner_draft
ON cup_games.winner_abbrev = winner_draft.team_abbrev
AND cup_games.season = winner_draft.season
LEFT JOIN draft AS loser_draft
ON cup_games.loser_abbrev = loser_draft.team_abbrev
AND cup_games.season = loser_draft.season
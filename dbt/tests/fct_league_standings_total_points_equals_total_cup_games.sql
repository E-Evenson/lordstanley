WITH results AS (
    SELECT season, SUM(points) AS total_points, SUM(games_played) / 2 AS total_games_played -- There are two teams in every game played, so divide the sum by 2
    FROM {{ ref('fct_league_standings') }}
    GROUP BY season
),
completed AS (
    SELECT season, COUNT(id) AS games_played
    FROM {{ source('intermediate', 'int_cup_possession') }}
    WHERE game_state IN ('OFF', 'FINAL')
    GROUP BY season
)
SELECT completed.season
FROM completed
JOIN results
    ON completed.season = results.season
WHERE completed.games_played <> results.total_points
OR completed.games_played <> results.total_games_played 

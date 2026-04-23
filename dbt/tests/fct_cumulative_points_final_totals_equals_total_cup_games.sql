WITH end_dates AS (
    SELECT season, MAX(game_date) as end_date
    FROM {{ ref('fct_cumulative_points') }}
    GROUP BY season
),
results AS (
    SELECT cum_points.season, SUM(cumulative_points) AS total_points
    FROM {{ ref('fct_cumulative_points') }} AS cum_points
    INNER JOIN end_dates
    ON cum_points.season = end_dates.season
    AND cum_points.game_date = end_dates.end_date
    GROUP BY cum_points.season
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

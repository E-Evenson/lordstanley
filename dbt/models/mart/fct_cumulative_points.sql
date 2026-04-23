-- Collapse same-date, same-owner games to handle owners playing against themselves
WITH games_collapsed AS (
    SELECT season, game_date, owner, SUM(is_win) as daily_wins
    FROM {{ ref('int_cup_games_with_owners_unpivoted') }}
    GROUP BY season, game_date, owner
),
cumulative_points AS (
    SELECT season, game_date, owner, SUM(daily_wins) OVER (PARTITION BY season, owner ORDER BY game_date) AS points
    FROM games_collapsed
),
owners AS (
    SELECT DISTINCT season, owner
    FROM {{ ref('drafts') }}
),
all_game_dates AS (
    SELECT DISTINCT season, game_date
    FROM {{ ref('int_cup_games_with_owners_unpivoted') }}
),
-- Ensure every owner has a row for every game date
game_grid AS (
    SELECT all_game_dates.season, all_game_dates.game_date, owners.owner
    FROM all_game_dates
    LEFT JOIN owners
        ON all_game_dates.season = owners.season
)
SELECT game_grid.season, game_grid.game_date, game_grid.owner, COALESCE(LAST_VALUE(cumulative_points.points IGNORE NULLS) OVER (PARTITION BY game_grid.season, game_grid.owner ORDER BY game_grid.game_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) AS cumulative_points
FROM game_grid
LEFT JOIN cumulative_points
ON game_grid.season = cumulative_points.season
    AND game_grid.game_date = cumulative_points.game_date
    AND game_grid.owner = cumulative_points.owner
ORDER BY game_date ASC
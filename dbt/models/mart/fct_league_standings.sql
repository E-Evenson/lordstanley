WITH unique_owners AS (
    SELECT DISTINCT owner, season
    FROM {{ ref('drafts') }}
),
standings AS (
    SELECT drafts.season, drafts.owner, SUM(is_win) AS points, COUNT(all_games.owner) AS games_played, ROUND(SUM(is_win) / COUNT(all_games.owner) * 100, 1) AS win_percentage
    FROM unique_owners AS drafts
    LEFT JOIN {{ ref('int_cup_games_with_owners_unpivoted') }} AS all_games
    ON drafts.owner = all_games.owner
        AND drafts.season = all_games.season
    GROUP BY drafts.season, drafts.owner
)
SELECT season, RANK() OVER(ORDER BY points DESC, games_played ASC) AS position, owner, points, games_played, win_percentage
FROM standings
ORDER BY position ASC
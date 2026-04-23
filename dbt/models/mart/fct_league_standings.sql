WITH unique_owners AS (
    SELECT DISTINCT owner, season
    FROM {{ ref('drafts') }}
),
standings AS (
    SELECT drafts.season, drafts.owner, COALESCE(SUM(is_win), 0) AS points, COALESCE(COUNT(all_games.owner), 0) AS games_played, COALESCE(ROUND(SUM(is_win) / COUNT(all_games.owner) * 100, 1), 0) AS win_percentage
    FROM unique_owners AS drafts
    LEFT JOIN {{ ref('int_cup_games_with_owners_unpivoted') }} AS all_games
    ON drafts.owner = all_games.owner
        AND drafts.season = all_games.season
    GROUP BY drafts.season, drafts.owner
)
SELECT season, RANK() OVER(PARTITION BY season ORDER BY points DESC, games_played ASC, owner ASC) AS position, owner, points, games_played, win_percentage
FROM standings
ORDER BY position ASC
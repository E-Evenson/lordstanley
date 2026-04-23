WITH 
team_stats AS (
    SELECT drafts.season, drafts.owner, drafts.team_abbrev, COALESCE(SUM(is_win), 0) AS points, COUNT(all_games.team_abbrev) AS games_played
    FROM  {{ ref('drafts') }} AS drafts
    LEFT JOIN {{ ref('int_cup_games_with_owners_unpivoted') }} AS all_games
    ON drafts.team_abbrev = all_games.team_abbrev
        AND drafts.season = all_games.season
    GROUP BY drafts.season, drafts.owner, drafts.team_abbrev
)
SELECT season, RANK() OVER(PARTITION BY season, owner ORDER BY points DESC, games_played ASC, team_abbrev ASC) AS position, team_abbrev, points, games_played, owner
FROM team_stats
ORDER BY position ASC

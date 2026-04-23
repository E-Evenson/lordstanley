SELECT DISTINCT
    schedule.currentSeason AS season,
    game.id AS id,
    game.gameType AS game_type,
    game.gameDate AS game_date,
    game.startTimeUTC AS start_time,
    game.gameState AS game_state,
    game.awayTeam.abbrev AS away_team_abbrev,
    game.awayTeam.score AS away_team_score,
    game.homeTeam.abbrev AS home_team_abbrev,
    game.homeTeam.score AS home_team_score,
    CASE
        WHEN game.gameState NOT IN ('OFF', 'FINAL') THEN NULL
        WHEN game.homeTeam.score > game.awayTeam.score THEN game.homeTeam.abbrev
        ELSE game.awayTeam.abbrev
    END AS winner_abbrev,
    CASE
        WHEN game.gameState NOT IN ('OFF', 'FINAL') THEN NULL
        WHEN game.homeTeam.score > game.awayTeam.score THEN game.awayTeam.abbrev
        ELSE game.homeTeam.abbrev
    END AS loser_abbrev
FROM {{ source('raw', 'schedule') }} AS schedule
CROSS JOIN UNNEST(schedule.games) as game -- Use CROSS JOIN for clarity
WHERE game.gameType = 2
ORDER BY game.id ASC
SELECT DISTINCT
    currentSeason AS season,
    id AS id,
    gameType AS game_type,
    gameDate AS game_date,
    gameState AS game_state,
    awayTeam.abbrev AS away_team_abbrev,
    awayTeam.score AS away_team_score,
    homeTeam.abbrev AS home_team_abbrev,
    homeTeam.score AS home_team_score,
    CASE
        WHEN gameState NOT IN ('OFF', 'FINAL') THEN NULL
        WHEN homeTeam.score > awayTeam.score THEN homeTeam.abbrev
        ELSE awayTeam.abbrev
    END AS winner_abbrev,
    CASE
        WHEN gameState NOT IN ('OFF', 'FINAL') THEN NULL
        WHEN homeTeam.score > awayTeam.score THEN awayTeam.abbrev
        ELSE homeTeam.abbrev
    END AS loser_abbrev
FROM `lord-stanley.raw.schedule` AS schedule,
UNNEST(games) as game
WHERE gameType = 2
ORDER BY id ASC
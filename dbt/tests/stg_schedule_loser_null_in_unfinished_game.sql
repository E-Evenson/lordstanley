SELECT id, game_state, loser_abbrev
FROM {{ ref('stg_schedule') }}
WHERE game_state NOT IN ('OFF', 'FINAL')
AND loser_abbrev IS NOT NULL
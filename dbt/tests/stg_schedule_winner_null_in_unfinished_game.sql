SELECT id, game_state, winner_abbrev
FROM {{ ref('stg_schedule') }}
WHERE game_state NOT IN ('OFF', 'FINAL')
AND winner_abbrev IS NOT NULL
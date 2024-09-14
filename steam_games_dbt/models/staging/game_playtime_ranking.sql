{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
),
ranked_games AS (
    SELECT 
        name,
        average_playtime_forever,
        ROW_NUMBER() OVER (ORDER BY average_playtime_forever DESC) AS rank
    FROM source
    WHERE average_playtime_forever > 0  -- Exclude games with 0 or negative playtime
)
SELECT 
    rank,
    name,
    average_playtime_forever AS avg_playtime_hours
FROM ranked_games
WHERE rank <= 100  -- Top 100 games
ORDER BY rank

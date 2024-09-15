{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
),
filtered_games AS (
    SELECT 
        name,
        positive,
        negative,
        CAST(positive AS FLOAT) / NULLIF(positive + negative, 0) AS score_ratio
    FROM source
    WHERE positive IS NOT NULL 
      AND negative IS NOT NULL 
      AND (positive + negative) > 0  -- Only include games with at least one vote
)

SELECT 
    name,
    positive,
    negative,
    LEAST(GREATEST(score_ratio, 0), 1) AS score_ratio  -- Ensure score_ratio is between 0 and 1
FROM filtered_games

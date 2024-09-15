{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
)

SELECT 
    name,
    positive,
    negative,
    CASE
        WHEN positive + negative = 0 THEN NULL
        ELSE LEAST(GREATEST(CAST(positive AS FLOAT) / NULLIF(positive + negative, 0), 0), 1)
    END AS score_ratio
FROM source

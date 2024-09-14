{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
)

SELECT 
    name,
    positive,
    negative,
    CAST(positive AS FLOAT) / NULLIF(positive + negative, 0) AS score_ratio
FROM source

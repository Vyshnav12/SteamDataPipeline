{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
)

SELECT 
    CASE 
        WHEN support_email THEN 'Yes'
        ELSE 'No'
    END AS has_support_email,
    COUNT(*) AS game_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM source
GROUP BY support_email

{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
),
email_counts AS (
    SELECT 
        support_email AS has_support_email,
        COUNT(*) AS game_count
    FROM source
    GROUP BY support_email
)
SELECT 
    CASE 
        WHEN has_support_email THEN 'Yes'
        ELSE 'No'
    END AS has_support_email,
    game_count,
    ROUND(game_count * 100.0 / SUM(game_count) OVER(), 2) AS percentage
FROM email_counts
ORDER BY has_support_email DESC

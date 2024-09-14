{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
),
parsed_dates AS (
    SELECT 
        CASE 
            WHEN LENGTH(release_date) > 8 THEN TRY_CAST(strptime(release_date, '%b %d, %Y') AS DATE)
            WHEN LENGTH(release_date) = 8 THEN TRY_CAST(strptime(release_date || ' 01', '%b %Y %d') AS DATE)
            WHEN LENGTH(release_date) = 7 THEN TRY_CAST(strptime(release_date || ' 01', '%b %Y %d') AS DATE)
            ELSE NULL
        END AS parsed_date
    FROM source
    WHERE release_date != ''
)
SELECT 
    DATE_TRUNC('month', parsed_date) AS release_month,
    COUNT(*) AS game_count
FROM parsed_dates
WHERE parsed_date IS NOT NULL
GROUP BY release_month
ORDER BY release_month

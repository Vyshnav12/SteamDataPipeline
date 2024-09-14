{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
),
dev_split AS (
    SELECT 
        trim(regexp_split_to_table(replace(replace(developers, '[', ''), ']', ''), ',')) AS developer
    FROM source
    WHERE developers != ''
),
pub_split AS (
    SELECT 
        trim(regexp_split_to_table(replace(replace(publishers, '[', ''), ']', ''), ',')) AS publisher
    FROM source
    WHERE publishers != ''
),
dev_counts AS (
    SELECT 
        developer,
        COUNT(*) AS game_count
    FROM dev_split
    WHERE developer != ''
    GROUP BY developer
),
pub_counts AS (
    SELECT 
        publisher,
        COUNT(*) AS game_count
    FROM pub_split
    WHERE publisher != ''
    GROUP BY publisher
)
SELECT 
    'developer' AS type, 
    developer AS name, 
    game_count
FROM (
    SELECT * FROM dev_counts
    ORDER BY game_count DESC
    LIMIT 10
)

UNION ALL

SELECT 
    'publisher' AS type, 
    publisher AS name, 
    game_count
FROM (
    SELECT * FROM pub_counts
    ORDER BY game_count DESC
    LIMIT 10
)

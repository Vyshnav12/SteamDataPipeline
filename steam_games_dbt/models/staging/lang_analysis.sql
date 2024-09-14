{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
),
language_split AS (
    SELECT 
        trim(regexp_split_to_table(replace(replace(supported_languages, '[', ''), ']', ''), ',')) AS language
    FROM source
    WHERE supported_languages != ''
),
language_counts AS (
    SELECT 
        language,
        COUNT(*) AS game_count
    FROM language_split
    WHERE language != ''
    GROUP BY language
)
SELECT * FROM language_counts
ORDER BY game_count DESC

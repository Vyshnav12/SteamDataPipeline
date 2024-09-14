{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM games
)

SELECT *
FROM source
WHERE 
    contains(genres, 'Action')  -- Example genre filter
    AND contains(categories, 'Multi-player')  -- Example category filter

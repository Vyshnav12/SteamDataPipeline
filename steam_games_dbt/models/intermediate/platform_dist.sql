{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM games
)

SELECT 
    SUM(CASE WHEN windows THEN 1 ELSE 0 END) AS windows_count,
    SUM(CASE WHEN mac THEN 1 ELSE 0 END) AS mac_count,
    SUM(CASE WHEN linux THEN 1 ELSE 0 END) AS linux_count
FROM source

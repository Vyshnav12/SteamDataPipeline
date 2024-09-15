-- This is now an analysis, not a test
SELECT
    name,
    average_playtime_forever,
    median_playtime_forever,
    average_playtime_forever - median_playtime_forever AS playtime_difference
FROM {{ ref('dim_games') }}
WHERE average_playtime_forever > median_playtime_forever
  AND average_playtime_forever > 0
  AND median_playtime_forever > 0
ORDER BY playtime_difference DESC
LIMIT 100  -- Limit to top 100 games with largest difference
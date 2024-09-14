WITH source AS (
    SELECT * FROM games  -- Direct reference to the table
)

SELECT
    name,
    release_date,
    required_age,
    price,
    dlc_count,
    support_email,
    windows,
    mac,
    linux,
    metacritic_score,
    achievements,
    recommendations,
    json(supported_languages) AS supported_languages,
    json(full_audio_languages) AS full_audio_languages,
    json(developers) AS developers,
    json(publishers) AS publishers,
    json(categories) AS categories,
    json(genres) AS genres,
    user_score,
    score_rank,
    positive,
    negative,
    estimated_owners,
    average_playtime_forever,
    average_playtime_2weeks,
    median_playtime_forever,
    median_playtime_2weeks,
    peak_ccu
FROM source

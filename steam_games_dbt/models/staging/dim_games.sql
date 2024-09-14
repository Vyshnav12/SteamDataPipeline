WITH stg_games AS (
    SELECT * FROM {{ ref('stg_games') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS game_id,  -- Create a surrogate key
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
    supported_languages,
    full_audio_languages,
    developers,
    publishers,
    categories,
    genres,
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
FROM stg_games

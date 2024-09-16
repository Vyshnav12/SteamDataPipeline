import polars as pl
import json

# Load the JSON data
with open('data/steam_games.json', 'r') as f:
    data = json.load(f)

# Initialize an empty list to hold processed game data
data_list = [
    {
        'app_id': app_id,
        'name': game_data.get('name', ''),
        'release_date': game_data.get('release_date', ''),
        'required_age': game_data.get('required_age', 0),
        'price': game_data.get('price', 0.0),
        'dlc_count': game_data.get('dlc_count', 0),
        'support_email': game_data.get('support_email', False),
        'windows': game_data.get('windows', False),
        'mac': game_data.get('mac', False),
        'linux': game_data.get('linux', False),
        'metacritic_score': game_data.get('metacritic_score', 0),
        'achievements': game_data.get('achievements', 0),
        'recommendations': game_data.get('recommendations', 0),
        'supported_languages': ', '.join(game_data.get('supported_languages', [])),
        'full_audio_languages': ', '.join(game_data.get('full_audio_languages', [])),
        'developers': ', '.join(game_data.get('developers', [])),
        'publishers': ', '.join(game_data.get('publishers', [])),
        'categories': ', '.join(game_data.get('categories', [])),
        'genres': ', '.join(game_data.get('genres', [])),
        'user_score': game_data.get('user_score', 0),
        'score_rank': game_data.get('score_rank', ''),
        'positive': game_data.get('positive', 0),
        'negative': game_data.get('negative', 0),
        'estimated_owners': game_data.get('estimated_owners', ''),
        'average_playtime_forever': game_data.get('average_playtime_forever', 0),
        'average_playtime_2weeks': game_data.get('average_playtime_2weeks', 0),
        'median_playtime_forever': game_data.get('median_playtime_forever', 0),
        'median_playtime_2weeks': game_data.get('median_playtime_2weeks', 0),
        'peak_ccu': game_data.get('peak_ccu', 0)
    }
    for app_id, game_data in data.items()
]

# Define the schema based on the provided types
schema = {
    'app_id': pl.Utf8,
    'name': pl.Utf8,
    'release_date': pl.Utf8,
    'required_age': pl.Int64,
    'price': pl.Float64,
    'dlc_count': pl.Int64,
    'support_email': pl.Boolean,
    'windows': pl.Boolean,
    'mac': pl.Boolean,
    'linux': pl.Boolean,
    'metacritic_score': pl.Int64,
    'achievements': pl.Int64,
    'recommendations': pl.Int64,
    'supported_languages': pl.Utf8,
    'full_audio_languages': pl.Utf8,
    'developers': pl.Utf8,
    'publishers': pl.Utf8,
    'categories': pl.Utf8,
    'genres': pl.Utf8,
    'user_score': pl.Int64,
    'score_rank': pl.Utf8,
    'positive': pl.Int64,
    'negative': pl.Int64,
    'estimated_owners': pl.Utf8,
    'average_playtime_forever': pl.Int64,
    'average_playtime_2weeks': pl.Int64,
    'median_playtime_forever': pl.Int64,
    'median_playtime_2weeks': pl.Int64,
    'peak_ccu': pl.Int64
}

# Convert to Polars DataFrame with the specified schema
df = pl.DataFrame(data_list, schema=schema)

# Function to generate additional DataFrames
def generate_dataframes(df):
    dataframes = {
        'genre_counts': (
            df
            .select(pl.col('genres').str.split(',').alias('genre_list'))
            .explode('genre_list')
            .select(pl.col('genre_list').str.replace_all(r'^\s+|\s+$', '').alias('genre'))
            .group_by('genre')
            .agg(pl.len().alias('count'))
            .sort('count', descending=True)
        ),
        'avg_price_by_genre': (
            df
            .select(pl.col('genres').str.split(',').alias('genre_list'), pl.col('price'))
            .explode('genre_list')
            .with_columns(pl.col('genre_list').str.replace_all(r'^\s+|\s+$', '').alias('genre'))
            .group_by('genre')
            .agg(pl.mean('price').alias('avg_price'))
            .sort('avg_price', descending=True)
        ),
        'top_10_dlc': (
            df
            .select(['name', 'dlc_count'])
            .unique(subset=['name'])
            .sort('dlc_count', descending=True)
            .head(10)
        ),
        'top_10_peak_ccu': (
            df
            .sort('peak_ccu', descending=True)
            .select(['name', 'peak_ccu'])
            .head(10)
        ),
        'platform_distribution': (
            df
            .select([
                pl.col('windows').alias('Windows'),
                pl.col('mac').alias('Mac'),
                pl.col('linux').alias('Linux')
            ])
            .sum()
        ),
        'top_10_languages': (
            df
            .select(pl.col('supported_languages').str.split(',').alias('language_list'))
            .explode('language_list')
            .select(pl.col('language_list').str.replace_all(r'^\s+|\s+$', '').alias('language'))
            .group_by('language')
            .agg(pl.len().alias('count'))
            .sort('count', descending=True)
            .head(10)
        ),
        'top_10_developers': (
            df
            .select(pl.col('developers').str.split(';').alias('developer_list'), pl.col('price'))
            .explode('developer_list')
            .select(pl.col('developer_list').str.replace_all(r'^\s+|\s+$', '').alias('developer'), 'price')
            .filter(pl.col('developer') != '')
            .group_by('developer')
            .agg([
                pl.len().alias('game_count'),
                pl.mean('price').alias('average_price')
            ])
            .sort('game_count', descending=True)
            .head(10)
        ),
        'games_per_year': (
            df
            .with_columns(pl.col('release_date').str.extract(r', (\d{4})$', 1).cast(pl.Int64).alias('release_year'))
            .group_by('release_year')
            .agg(pl.len().alias('game_count'))
            .sort('release_year')
        ),
        'games_highest_ownership': (
            df
            .with_columns(pl.col('estimated_owners').str.replace_all(',', '').str.extract(r'(\d+)$', 1).cast(pl.Int64).alias('estimated_owners_num'))
            .sort('estimated_owners_num', descending=True)
            .select(['name', 'estimated_owners_num'])
            .head(10)
        ),
        'avg_positive_negative_by_genre': (
            df
            .select(pl.col('genres').str.split(',').alias('genre_list'), pl.col('positive'), pl.col('negative'))
            .filter(pl.col('positive') > 0)
            .filter(pl.col('negative') > 0)
            .explode('genre_list')
            .select(pl.col('genre_list').str.replace_all(r'^\s+|\s+$', '').alias('genre'), 'positive', 'negative')
            .group_by('genre')
            .agg([
                pl.mean('positive').alias('average_positive_reviews'),
                pl.mean('negative').alias('average_negative_reviews'),
                pl.len().alias('game_count')
            ])
            .sort('average_positive_reviews', descending=True)
            .filter(pl.col('game_count') >= 10)
        ),
        'price_distribution': df.select(['price']).describe(),
        'top_developers_user_score': (
            df
            .select(pl.col('developers').str.split(',').alias('developer_list'), pl.col('recommendations'))
            .explode('developer_list')
            .select(pl.col('developer_list').str.replace_all(r'^\s+|\s+$', '').alias('developer'), 'recommendations')
            .group_by('developer')
            .agg([
                pl.mean('recommendations').alias('average_recommendations'),
                pl.len().alias('game_count')
            ])
            .filter(pl.col('game_count') >= 10)
            .sort('average_recommendations', descending=True)
            .head(10)
        ),
        'age_distribution': (
            df
            .select(pl.col('required_age').fill_null(0).cast(pl.Int64).alias('required_age'))
            .with_columns([
                (
                    pl.when(pl.col('required_age').is_between(-1, 8)).then(pl.lit('1. Everyone'))
                    .when(pl.col('required_age').is_between(8, 12)).then(pl.lit('2. PG'))
                    .when(pl.col('required_age').is_between(12, 16)).then(pl.lit('3. Teen'))
                    .when(pl.col('required_age') >= 17).then(pl.lit('4. Mature'))
                    .otherwise(pl.lit('Everyone'))
                ).alias('age_category')
            ])
            .group_by('age_category')
            .agg(pl.len().alias('number_of_games'))
            .sort('age_category')
        )
    }
    return dataframes

# Generate all DataFrames
dataframes = generate_dataframes(df)

# Save the main DataFrame and additional DataFrames to Parquet files
df.write_parquet('./parquet_tables/steam_games.parquet')
for name, dataframe in dataframes.items():
    dataframe.write_parquet(f'./parquet_tables/{name}.parquet')

print("All data has been successfully transformed and saved to Parquet files.")



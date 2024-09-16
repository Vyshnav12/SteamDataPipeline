import polars as pl
import psycopg2
from psycopg2.extras import execute_values

# Function to save Polars DataFrame to PostgreSQL
def save_to_postgres(df, table_name, conn_params):
    type_mapping = {
        'Int64': 'BIGINT',
        'Int32': 'INTEGER',
        'Int16': 'SMALLINT',
        'Int8': 'SMALLINT',
        'Float64': 'DOUBLE PRECISION',
        'Float32': 'REAL',
        'Utf8': 'TEXT',
        'Boolean': 'BOOLEAN',
        'Date': 'DATE',
        'Datetime': 'TIMESTAMP',
        'Time': 'TIME',
    }
    
    columns = ', '.join([f"{col} {type_mapping.get(str(dtype), 'TEXT')}" for col, dtype in zip(df.columns, df.dtypes)])
    data = [tuple(row) for row in df.rows()]
    columns_str = ', '.join(df.columns)
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
    
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
                execute_values(cur, query, data)
                conn.commit()
    except Exception as e:
        print(f"Error saving to PostgreSQL: {e}")

# Connection parameters for PostgreSQL
conn_params = {
    'dbname': 'Steam_Games',
    'user': 'postgres',
    'password': 'steamuser',
    'host': 'localhost',
    'port': '6060'
}

# Load the main DataFrame and additional DataFrames from Parquet files
df = pl.read_parquet('./parquet_tables/steam_games.parquet')
dataframes = {
    'genre_counts': pl.read_parquet('./parquet_tables/genre_counts.parquet'),
    'avg_price_by_genre': pl.read_parquet('./parquet_tables/avg_price_by_genre.parquet'),
    'top_10_dlc': pl.read_parquet('./parquet_tables/top_10_dlc.parquet'),
    'top_10_peak_ccu': pl.read_parquet('./parquet_tables/top_10_peak_ccu.parquet'),
    'platform_distribution': pl.read_parquet('./parquet_tables/platform_distribution.parquet'),
    'top_10_languages': pl.read_parquet('./parquet_tables/top_10_languages.parquet'),
    'top_10_developers': pl.read_parquet('./parquet_tables/top_10_developers.parquet'),
    'games_per_year': pl.read_parquet('./parquet_tables/games_per_year.parquet'),
    'games_highest_ownership': pl.read_parquet('./parquet_tables/games_highest_ownership.parquet'),
    'avg_positive_negative_by_genre': pl.read_parquet('./parquet_tables/avg_positive_negative_by_genre.parquet'),
    'price_distribution': pl.read_parquet('./parquet_tables/price_distribution.parquet'),
    'top_developers_user_score': pl.read_parquet('./parquet_tables/top_developers_user_score.parquet'),
    'age_distribution': pl.read_parquet('./parquet_tables/age_distribution.parquet')
}

# Save the main DataFrame to PostgreSQL
save_to_postgres(df, 'steam_games', conn_params)

# Save all additional DataFrames to PostgreSQL
for table_name, dataframe in dataframes.items():
    save_to_postgres(dataframe, table_name, conn_params)

print("All data has been successfully loaded into PostgreSQL.")
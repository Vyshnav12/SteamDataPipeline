import polars as pl
import psycopg2
from psycopg2.extras import execute_values
import os
import socket
import time

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
    'dbname': os.getenv('POSTGRES_DB', 'Steam_Games'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'steamuser'),
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': os.getenv('POSTGRES_PORT', '6060')
}

# Add this function for debugging
def debug_connection():
    print(f"Attempting to connect to PostgreSQL at {os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}")
    try:
        ip_address = socket.gethostbyname(os.getenv('POSTGRES_HOST'))
        print(f"Resolved {os.getenv('POSTGRES_HOST')} to IP: {ip_address}")
    except socket.gaierror:
        print(f"Failed to resolve {os.getenv('POSTGRES_HOST')}")

def connect_with_retry(max_retries=30, delay=2):
    conn_params = {
        'dbname': os.getenv('POSTGRES_DB', 'Steam_Games'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'steamuser'),
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': os.getenv('POSTGRES_PORT', '5432')
    }
    
    for attempt in range(max_retries):
        try:
            debug_connection()
            conn = psycopg2.connect(**conn_params)
            print("Successfully connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Unable to connect to PostgreSQL.")
                raise

# Use the retry mechanism to establish a connection
try:
    conn = connect_with_retry()
    # Your existing code to save data goes here
    # ...
    conn.close()
except Exception as e:
    print(f"Error: {e}")

# Set the base path for the parquet files
base_path = '/app/parquet_tables'

# Load the main DataFrame
df = pl.read_parquet(os.path.join(base_path, 'steam_games.parquet'))

# Load additional DataFrames
dataframes = {
    'genre_counts': pl.read_parquet(os.path.join(base_path, 'genre_counts.parquet')),
    'avg_price_by_genre': pl.read_parquet(os.path.join(base_path, 'avg_price_by_genre.parquet')),
    'top_10_dlc': pl.read_parquet(os.path.join(base_path, 'top_10_dlc.parquet')),
    'top_10_peak_ccu': pl.read_parquet(os.path.join(base_path, 'top_10_peak_ccu.parquet')),
    'platform_distribution': pl.read_parquet(os.path.join(base_path, 'platform_distribution.parquet')),
    'top_10_languages': pl.read_parquet(os.path.join(base_path, 'top_10_languages.parquet')),
    'top_10_developers': pl.read_parquet(os.path.join(base_path, 'top_10_developers.parquet')),
    'games_per_year': pl.read_parquet(os.path.join(base_path, 'games_per_year.parquet')),
    'games_highest_ownership': pl.read_parquet(os.path.join(base_path, 'games_highest_ownership.parquet')),
    'avg_positive_negative_by_genre': pl.read_parquet(os.path.join(base_path, 'avg_positive_negative_by_genre.parquet')),
    'price_distribution': pl.read_parquet(os.path.join(base_path, 'price_distribution.parquet')),
    'top_developers_user_score': pl.read_parquet(os.path.join(base_path, 'top_developers_user_score.parquet')),
    'age_distribution': pl.read_parquet(os.path.join(base_path, 'age_distribution.parquet'))
}

# Save the main DataFrame to PostgreSQL
save_to_postgres(df, 'steam_games', conn_params)

# Save all additional DataFrames to PostgreSQL
for table_name, dataframe in dataframes.items():
    save_to_postgres(dataframe, table_name, conn_params)

print("All data has been successfully loaded into PostgreSQL.")
import duckdb
import psycopg2
import sys
import os

def log_error(message):
    print(f"Error: {message}")
    sys.exit(1)

def map_duckdb_to_postgres_type(duckdb_type):
    type_mapping = {
        'HUGEINT': 'BIGINT',  # Map HUGEINT to BIGINT
        'DOUBLE': 'DOUBLE PRECISION',  # Add DOUBLE PRECISION mapping
        # Add more mappings if needed
    }
    return type_mapping.get(duckdb_type.upper(), duckdb_type)

# Check if the DuckDB file exists
duckdb_file = os.path.join('duckdb', 'steam_games.duckdb')
if not os.path.exists(duckdb_file):
    log_error(f"DuckDB file not found: {duckdb_file}")

# Connect to DuckDB
try:
    duck_conn = duckdb.connect(duckdb_file)
    print(f"Connected to DuckDB successfully: {duckdb_file}")
    
    # List all tables
    tables = duck_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"Tables in DuckDB: {tables}")
except Exception as e:
    log_error(f"Failed to connect to DuckDB: {e}")

# Connect to PostgreSQL
try:
    pg_conn = psycopg2.connect(
        dbname="steam_games",
        user="postgres",
        password="Your_Password",
        host="localhost",
        port="6060"
    )
    pg_conn.autocommit = False  # Disable autocommit to handle transactions manually
    pg_cursor = pg_conn.cursor()
    print("Connected to PostgreSQL successfully")
except Exception as e:
    log_error(f"Failed to connect to PostgreSQL: {e}")

# Export data
for table in tables:
    table_name = table[0]
    print(f"Exporting table: {table_name}")

    try:
        # Start a new transaction for each table
        pg_cursor.execute("BEGIN")

        # Get table schema from DuckDB
        schema = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
        
        # Create table in PostgreSQL
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_table_sql += ", ".join([f"{col[0]} {map_duckdb_to_postgres_type(col[1])}" for col in schema])
        create_table_sql += ")"
        pg_cursor.execute(create_table_sql)
        print(f"Created table {table_name} in PostgreSQL")

        # Export data from DuckDB to PostgreSQL
        data = duck_conn.execute(f"SELECT * FROM {table_name}").fetchall()
        if data:
            placeholders = ", ".join(["%s"] * len(schema))
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            pg_cursor.executemany(insert_sql, data)
            print(f"Inserted {len(data)} rows into {table_name}")
        else:
            print(f"No data found in table {table_name}")

    except Exception as e:
        print(f"Error exporting table {table_name}: {e}")
        print("Continuing with next table...")

    try:
        pg_conn.commit()
        print(f"Committed data for table {table_name}")
    except Exception as e:
        print(f"Failed to commit data for table {table_name}: {e}")
        pg_conn.rollback()
        print(f"Rolled back transaction for table {table_name}")

try:
    pg_conn.commit()
    print("All data committed to PostgreSQL")
except Exception as e:
    log_error(f"Failed to commit data to PostgreSQL: {e}")

finally:
    pg_cursor.close()
    pg_conn.close()
    duck_conn.close()
    print("All connections closed")

print("Export completed successfully")

#!/bin/sh
echo "Starting run_duckdb_setup.sh"
echo "Current directory: $(pwd)"
echo "Contents of current directory:"
ls -la
echo "Contents of /app/duckdb:"
ls -la /app/duckdb
echo "Running Python script..."
python /app/duckdb/steam_games_db_setup.py
echo "Script execution completed"
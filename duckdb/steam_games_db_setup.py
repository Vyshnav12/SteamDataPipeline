import duckdb
import json
import os
import sys
from tqdm import tqdm

def load_data(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file {file_path}")
        sys.exit(1)

def clean_data(games_data):
    cleaned = []
    for game_id, game_data in tqdm(games_data.items(), desc="Cleaning data"):
        cleaned.append((
            game_data.get('name', ''),
            game_data.get('release_date', ''),
            game_data.get('required_age', 0),
            game_data.get('price', 0.0),
            game_data.get('dlc_count', 0),
            game_data.get('support_email', False),
            game_data.get('windows', False),
            game_data.get('mac', False),
            game_data.get('linux', False),
            game_data.get('metacritic_score', 0),
            game_data.get('achievements', 0),
            game_data.get('recommendations', 0),
            json.dumps(game_data.get('supported_languages', [])),
            json.dumps(game_data.get('full_audio_languages', [])),
            json.dumps(game_data.get('developers', [])),
            json.dumps(game_data.get('publishers', [])),
            json.dumps(game_data.get('categories', [])),
            json.dumps(game_data.get('genres', [])),
            game_data.get('user_score', 0.0),
            game_data.get('score_rank', ''),
            game_data.get('positive', 0),
            game_data.get('negative', 0),
            game_data.get('estimated_owners', ''),
            game_data.get('average_playtime_forever', 0),
            game_data.get('average_playtime_2weeks', 0),
            game_data.get('median_playtime_forever', 0),
            game_data.get('median_playtime_2weeks', 0),
            game_data.get('peak_ccu', 0)
        ))
    return cleaned

def create_table(con):
    con.execute('''
        CREATE TABLE IF NOT EXISTS games (
            name VARCHAR,
            release_date VARCHAR,
            required_age INTEGER,
            price DECIMAL(10, 2),
            dlc_count INTEGER,
            support_email BOOLEAN,
            windows BOOLEAN,
            mac BOOLEAN,
            linux BOOLEAN,
            metacritic_score INTEGER,
            achievements INTEGER,
            recommendations INTEGER,
            supported_languages VARCHAR,
            full_audio_languages VARCHAR,
            developers VARCHAR,
            publishers VARCHAR,
            categories VARCHAR,
            genres VARCHAR,
            user_score DECIMAL(5, 2),
            score_rank VARCHAR,
            positive INTEGER,
            negative INTEGER,
            estimated_owners VARCHAR,
            average_playtime_forever INTEGER,
            average_playtime_2weeks INTEGER,
            median_playtime_forever INTEGER,
            median_playtime_2weeks INTEGER,
            peak_ccu INTEGER
        )
    ''')

def insert_data(con, data):
    batch_size = 1000
    for i in tqdm(range(0, len(data), batch_size), desc="Inserting data"):
        batch = data[i:i+batch_size]
        con.executemany('''
            INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', batch)

def main():
    print("Script is running!")
    
    base_dir = '/app'
    data_file = os.path.join(base_dir, 'data', 'steam_games.json')
    db_file = os.path.join(base_dir, 'duckdb', 'steam_games.duckdb')

    print(f"Loading data from {data_file}")
    data = load_data(data_file)

    cleaned_data = clean_data(data)
    print(f"Number of games: {len(cleaned_data)}")

    print(f"Connecting to database at {db_file}")
    con = duckdb.connect(db_file)

    try:
        con.begin()
        create_table(con)
        insert_data(con, cleaned_data)
        con.commit()
        print("Data inserted successfully")
    except Exception as e:
        con.rollback()
        print(f"An error occurred: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    main()

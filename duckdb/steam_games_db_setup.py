import duckdb
import json

with open('data/steam_games.json', 'r') as file:
    data = json.load(file)

def clean_data(games_data):
    games = [
        (
            int(game_id),
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
        )
        for game_id, game_data in games_data.items()
    ]
    return games

def create_table(data):
    con = duckdb.connect('duckdb/steam_games.duckdb')
    con.execute('''
        CREATE TABLE games (
            game_id INTEGER,
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
    con.close()
    
def insert_data(data):
    con = duckdb.connect('duckdb/steam_games.duckdb')
    con.executemany('''
        INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data)
    con.close()

if __name__ == "__main__":
    cleaned_data = clean_data(data)
    print(f"Number of elements in a single game's data: {len(cleaned_data[0])}")
    create_table(cleaned_data)
    insert_data(cleaned_data)
    con = duckdb.connect('duckdb/steam_games.duckdb')
    print(con.execute("SELECT * FROM games").fetchdf().head())
    con.close()

# Steam Games Data Pipeline

## DuckDB Setup (`steam_game_db_setup.py`)

This script sets up a DuckDB database with Steam games data. It performs the following operations:

1. Reads JSON data from `data/steam_games.json`.
2. Cleans and transforms the data.
3. Creates a `games` table in a DuckDB database.
4. Inserts the cleaned data into the `games` table.

### Usage

Run the script using Python:

```bash
python duckdb/steam_game_db_setup.py
```

This will create and populate a DuckDB database file at `duckdb/steam_games.duckdb`.

## DBT Models

The DBT project is structured as follows:

```
steam_games_dbt/
└── models/
    ├── staging/
    │   └── stg_games.sql
    ├── intermediate/
    │   ├── game_playtime_ranking.sql
    │   ├── game_filters.sql
    │   ├── platform_dist.sql
    │   ├── support_email_availability.sql
    │   ├── lang_analysis.sql
    │   ├── release_trends.sql
    │   └── user_score_ratio.sql
    └── marts/
        └── dim_games.sql
```

### Staging

- `stg_games.sql`: Initial transformation of raw data, including JSON parsing for certain fields.

### Intermediate

- `game_playtime_ranking.sql`: Ranks games based on average playtime.
- `game_filters.sql`: Demonstrates filtering games by genre and category.
- `platform_dist.sql`: Analyzes the distribution of games across different platforms.
- `support_email_availability.sql`: Examines the availability of support emails for games.
- `lang_analysis.sql`: Analyzes the languages supported by games.
- `release_trends.sql`: Tracks game release trends over time.
- `user_score_ratio.sql`: Calculates the ratio of positive to total user scores for each game.

### Marts

- `dim_games.sql`: Creates a dimension table for games with a surrogate key.

These models transform the raw steam game data into a more analysis-ready format.

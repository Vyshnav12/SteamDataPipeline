FROM python:3.12.5

COPY . /app
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /root/.dbt && chmod 777 /root/.dbt
COPY ./steam_games_dbt/profiles.yml /root/.dbt/profiles.yml

RUN chmod +x /app/duckdb/steam_games_db_setup.py
RUN chmod +x /app/run_duckdb.sh

ENTRYPOINT ["/bin/bash", "-c"]

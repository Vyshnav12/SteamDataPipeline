# Use Python 3.9 slim-buster as the base image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /usr/src/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt to the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the steam_games_dbt directory contents into the container
COPY ./steam_games_dbt /usr/src/app/steam_games_dbt

# Create .dbt directory and set permissions
RUN mkdir -p /root/.dbt && chmod 777 /root/.dbt

# Copy profiles.yml to the dbt configuration directory
COPY ./steam_games_dbt/profiles.yml /root/.dbt/profiles.yml

# Set the working directory to the steam_games_dbt folder
WORKDIR /usr/src/app/steam_games_dbt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Run dbt when the container launches
CMD ["dbt", "run"]

# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install python-dotenv

# Copy the entrypoint script into the container
COPY . .


# Set the entrypoint script
CMD ["python", "scraper/steam_scraper.py"]

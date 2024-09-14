# SteamDataPipeline (WIP)

<p align="center">
  <img src="https://img.shields.io/badge/Language-Python-blue" alt="Python" />
  <img src="https://img.shields.io/badge/Steam-API-171A21?logo=steam&logoColor=white" alt="Steam" />
  <img src="https://img.shields.io/badge/AWS-S3-28a745?logo=amazon-aws&logoColor=white" alt="AWS S3" />
  <img src="https://img.shields.io/badge/AWS-EC2-ff9900?logo=amazon-aws&logoColor=white" alt="AWS EC2" />
  <img src="https://img.shields.io/badge/Release-Coming%20Soon-yellow?logo=rocket&logoColor=white" alt="Release" />
  <img src="https://img.shields.io/github/license/Vyshnav12/dynamic-github-readme-updater" alt="License" />
</p>

This project aims to create a scalable data pipeline that collects and processes data from both Steam and SteamSpy, stores it in AWS S3, and sets up automation for regular updates using AWS Lambda. This pipeline can be used for analyzing trends in the gaming industry, creating visual dashboards, and more.

## Overview

The SteamDataPipeline integrates multiple components to scrape, store, and analyze data from the Steam platform. The scraper gathers game information and statistics from Steam's API and SteamSpy, then uploads the data to an S3 bucket. The entire process is automated using AWS services, and future plans include scheduling for regular database updates, modeling the data for easy querying, visualization, and reporting using tools like on AWS Lambda, DBT and Grafana.

### Features:
- **Scalable Scraper:** The Steam scraper is designed to handle large amounts of data while complying with API rate limits.
- **Modular Codebase:** The functions are split across multiple files to ensure modularity, allowing easy management and future extensions.
- **AWS Integration:** Collected data is stored on Amazon S3.
- **JSON Format:** Data is read from and written to JSON files for ease of use and compatibility.
- **Retry Logic and Backoff Strategies:** The scraper includes retry logic for handling API failures and rate-limiting.
- **Monitoring:** The pipeline utilizes logs to monitor the scraping process and AWS CloudWatch for further insights.

## Core Functionality

This pipeline is designed to efficiently scrape and process data from Steam and SteamSpy. It's a heavily modified version of [FronkonGames' Steam Games Scraper](https://github.com/FronkonGames/Steam-Games-Scraper), with significant enhancements for improved performance, scalability, and code readability.

### Key Features

- Modular architecture for easier maintenance and extensibility
- Optimized for high-performance data collection
- **Set-based lookups** for tracking `discarded` and `notreleased` AppIDs, improving the performance of AppID lookups.
- **Manifest tracking** to avoid redundant API calls by saving all processed AppIDs, ensuring efficient updates.
- Enhanced progress logging to display the percentage completion and elapsed time during the scraping process.
- Improved code structure and documentation

The project structure has been reorganized into logical components, facilitating easier updates and the addition of new functionalities.

## Technologies Used
- **Python 3.12.5**: The primary language for the scraper.
- **AWS S3**: To store the scraped data.
- **AWS EC2**: Used for running the scraper to handle the large Steam dataset.
- **AWS Lambda**: Planned for future automated updates.
- **PostgreSQL**: Planned for data warehousing.
- **DBT**: Planned for data modeling.
- **Grafana**: Planned for visualization.

## Project Structure

SteamDataPipeline/
├── src/
│   ├── utils.py                 # Data processing and S3 operations
│   ├── config.py                # Project configuration
│   ├── steam_scraper.py         # Steam API scraping logic
│   └── api.py                   # API interaction functions
├── tests/                       # Unit tests
│   ├── test_scraper.py          # Tests for scraper
│   ├── test_utils.py            # Tests for utility functions
│   └── test_api.py              # Tests for API functions
├── logs/                        # Log files
├── data/                        # Scraping output files
├── duckdb/                      # DuckDB related files
│   ├── sql.py                   # SQL operations script
│   ├── steam_games.duckdb       # DuckDB database file
│   └── test.ipynb               # Jupyter notebook for testing queries
├── proj_dbt/                    # DBT project folder
├── README.md                    # Project documentation
└── requirements.txt             # Required libraries

## Installation

To get started with SteamDataPipeline, you'll need to:

1. Clone the repository:
   ``` bash
    git clone https://github.com/Vyshnav12/SteamDataPipeline.git
   ```

2. Install the required dependencies:
    ``` bash
    pip install -r requirements.txt
    ```

3. Set up your AWS credentials for S3 access. You can do this by either:
   - Setting environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
   - Or by configuring the AWS CLI: `aws configure`

## Running the Scraper

Execute the scraper using:

```bash
python src/steam_scraper.py [arguments]
```
Use the argument `-h` to see all the arguments and their descriptions.

### Example

```bash
python src/steam_scraper.py --sleep 2 --retries 5 --autosave 100 --bucket my-steam-data-bucket
```

This sets a 2-second delay, 5 retries, saves every 100 entries, and uses the 'my-steam-data-bucket' S3 bucket.

### EC2 Background Execution

```bash
nohup python3 src/steam_scraper.py > logs/output.log 2>&1 &
```

Note: Default values are defined in `config.py`.

## Testing

This project uses Python's built-in `unittest` framework for testing. The tests are located in the `tests/` directory.

### Running Tests

To run all tests, use the following command from the project root directory:

```bash
python -m unittest discover tests
```

To run a specific test file, use:

```bash
python -m unittest tests/test_utils.py
```

Replace `test_utils.py` with the name of the test file you want to run.

## Data Storage

This project primarily relies on AWS S3 for data storage. However, you can modify the `utils.py` file to enable local storage if needed.

## Future Enhancements

I plan to expand SteamDataPipeline with the following features:
- **Automated scheduling**: Either AWS Lambda or cron-jobs will be integrated to automate the scraper, ensuring data is collected at regular intervals.
- **Data modeling**: DBT will be used to model and transform the data stored in S3 for easier analysis.
- **Data Warehousing**: PostgreSQL will be used for data warehousing.
- **Data visualization**: Grafana will be connected to the data pipeline to provide real-time insights into the scraped data.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgment

Special thanks to [Martin Bustos aka FronkonGames](https://github.com/FronkonGames) for providing the foundation that made this project possible.

## License

This project is licensed under the MIT License - see the LICENSE file for details.


# SteamDataPipeline (WIP)

This project aims to create a scalable data pipeline that collects and processes data from both Steam and SteamSpy, stores it in AWS S3, and sets up automation for regular updates using AWS Lambda. This pipeline can be used for analyzing trends in the gaming industry, creating visual dashboards, and more.

<div align="center">
  <img src="assets/banner.gif" width="80%" />
</div>

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

This pipeline is designed to efficiently scrape and process data from Steam and SteamSpy. It's built upon [FronkonGames' Steam Games Scraper](https://github.com/FronkonGames/Steam-Games-Scraper), with significant enhancements for improved performance, scalability, and code readability.

### Key Features

- Modular architecture for easier maintenance and extensibility
- Optimized for high-performance data collection
- Improved code structure and documentation

The project structure has been reorganized into logical components, facilitating easier updates and the addition of new functionalities.

## Technologies Used
- **Python 3.12.5**: The primary language for the scraper.
- **AWS S3**: To store the scraped data.
- **EC2**: Used for a one-time process to handle scraping the large Steam dataset. Future updates is planned to be scheduled using either cron jobs or Lambda.
- **DBT**: Planned for data modeling.
- **Grafana**: Planned for visualization.


## Project Structure
```
SteamDataPipeline/
├── src/
│   ├── utils.py                 # Data processing and S3 operations
│   ├── config.py                # Project configuration
│   ├── steam_scraper.py         # Steam API scraping logic
│   └── api.py                   # API interaction functions
├── logs/                        # Log files
├── data/                        # Scraping output files (WIP - currently being populated)
├── README.md                    # Project documentation
└── requirements.txt             # Required libraries
```

## Installation

To get started with SteamDataPipeline, you’ll need to:

1. Clone the repository:
   ``` bash
        git clone https://github.com/Vyshnav12/SteamDataPipeline.git
   ```

2. Install the required dependencies:
    ``` bash
        pip install -r requirements.txt
    ```

3. Set up your AWS credentials for S3 access in `~/.aws/credentials` (this project relies on S3 to store the data, no local data storage).

## Running the Scraper

To run the scraper, navigate to the `src` directory and run the following command:

- **Locally**: 
    ```bash
    cd src && python steam_scraper.py
    ```

- **On EC2 with Background Execution**:
    ```bash
   nohup python3 src/steam_scraper.py > logs/output.log 2>&1 &
   ```

The script will begin scraping data from Steam and SteamSpy, with all logs being stored in `logs/output.log` for review. The data will be uploaded to your specified S3 bucket which can be modified within `src/steam_scraper.py`.

## Future Enhancements

I plan to expand SteamDataPipeline with the following features:
- **Automated scheduling**: Either AWS Lambda or cron-jobs will be integrated to automate the scraper, ensuring data is collected at regular intervals.
- **Data modeling**: DBT will be used to model and transform the data stored in S3 for easier analysis.
- **Data Warehousing**: PostgreSQL will be used for data warehousing.
- **Data visualization**: Grafana will be connected to the data pipeline to provide real-time insights into the scraped data.

## Acknowledgment

Special thanks to [Martin Bustos aka FrokonGames](https://github.com/FronkonGames) for providing the foundation that made this project possible.

## License

This project is licensed under the MIT License - see the LICENSE file for details.


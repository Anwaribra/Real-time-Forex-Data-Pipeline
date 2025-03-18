# Real-time Forex Data Pipeline

## Project Overview
A data pipeline project that fetches real-time currency exchange rates from [Alpha Vantage API](https://www.alphavantage.co), processes the data, and stores it in Snowflake data warehouse. The pipeline is orchestrated using Apache Airflow.

## Features
- Real-time forex data fetching
- Data processing and cleaning
- Snowflake data warehouse integration
- Apache Airflow task scheduling
- Support for multiple currency pairs

## Project Structure
```
Real-time-Data-Pipeline/
├── config/                    # Configuration files
│   └── config.json           # API keys and settings
├── dags/                     # Airflow DAGs
│   └── dags/
│       └── forex_pipeline_dag.py
├── data_ingestion/           # Data fetching modules
│   ├── fetch_data.py         # API and mock data functions
│   ├── fetch_historical_data.py # Historical data fetching
│   └── cleanup.py            # Data cleanup utilities
├── data_storage/             # Data storage operations
│   ├── save_to_snowflake.py  # Snowflake integration
│   └── simplified_storage.py # Local storage fallback
├── data/                     # Data storage for all files
├── logs/                     # Log files
├── tests/                    # Test cases
└── requirements.txt          # Python dependencies
```

## Technologies Used
- Python 3.9
- Apache Airflow
- Snowflake
- Alpha Vantage API
- Snowflake (optional)
- Apache Airflow (optional)

## How It Works
1. **Data Collection**: Fetches from API or generates mock data
2. **Processing**: Validates and transforms the data
3. **Storage**: Saves to Snowflake or local files depending on availability

## Error Handling
- API rate limits → automatic mock data
- Snowflake unavailable → local CSV/JSON storage
- Data validation issues → proper error reporting

## Working With API Limits
The Alpha Vantage free tier has a 25 requests/day limit.
The pipeline detects rate limits and automatically switches to mock data.

## Usage Options

### Standalone Mode
```bash
# Run with mock data
python run_pipeline.py --mock

# View latest rates
python view_rates.py
```

### Airflow DAG Mode
Two options to run the Airflow DAG:

1. **Without Airflow installed**:
```bash
# Run the DAG without needing Airflow
python run_dag.py

## Data Flow
1. **Data Collection**: Fetch forex rates from Alpha Vantage API
2. **Data Processing**: Transform and clean the data
3. **Data Storage**: Save processed data to Snowflake

## Snowflake Integration
The pipeline creates the following in Snowflake:
- Database (if not exists)
- Schema (if not exists)
- FOREX_RATES table with columns:
  - from_currency
  - to_currency
  - exchange_rate
  - last_refreshed
  - timestamp
  - inserted_at

## Airflow Workflow
The pipeline is orchestrated using Apache Airflow with a simple but effective workflow:

![Airflow DAG Workflow](images/airflow_dag.png)

The workflow consists of two main tasks:
1. `fetch_forex_rates`: Retrieves currency exchange data from the API or generates mock data if needed
2. `process_and_store_data`: Processes the retrieved data and stores it in Snowflake or local storage

## Snowflake Data Visualization
The processed data can be viewed in Snowflake's interface, with proper staging and table creation:

![Snowflake Visualization](images/snowflake_view.png)

## Error Handling
The pipeline includes comprehensive error handling for:
- API connection issues
- Data validation
- Snowflake operations
- File operations


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
├── data_storage/             # Data storage operations
│   └── save_to_snowflake.py # Snowflake integration
├── data/                     # Temporary data storage
├── logs/                     # Log files
└── requirements.txt          # Python dependencies
```

## Technologies Used
- Python 3.9
- Apache Airflow
- Snowflake
- Alpha Vantage API



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

## Error Handling
The pipeline includes comprehensive error handling for:
- API connection issues
- Data validation
- Snowflake operations
- File operations


# Real-time Forex Data Pipeline

## Project Overview
A data pipeline project that fetches real-time currency exchange rates from [Alpha Vantage API](https://www.alphavantage.co), processes the data, and stores it in Snowflake data warehouse. The pipeline is orchestrated using Apache Airflow and can also be run in Docker containers.

## Features
- Real-time forex data fetching
- Historical data collection
- Data processing and cleaning
- Snowflake data warehouse integration
- Local storage fallback (JSON/CSV)
- Apache Airflow task scheduling
- Docker containerization
- Support for multiple currency pairs
- Rate viewing utility

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
│   ├── cleanup.py            # Data cleanup utilities
│   └── __init__.py
├── data_storage/             # Data storage operations
│   ├── save_to_snowflake.py  # Snowflake integration
│   ├── simplified_storage.py # Local storage fallback
├── Dockerfile                # Docker configuration
├── docker-compose.yaml       # Docker compose configuration
└── requirements.txt          # Python dependencies
```

## Technologies Used
- Python 3.9
- Apache Airflow 2.7.1
- Snowflake
- Alpha Vantage API
- Docker
- Pandas & NumPy
  
## Pipeline Architecture
### Snowflake Compute Warehouse
The pipeline uses a dedicated compute warehouse in Snowflake:

![Snowflake Compute Warehouse](config/icon/P1.png)
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

The data pipeline follows this architecture in Snowflake:

1. `FOREX_STAGE` - Initial data ingestion stage
2. `FOREX_RATES_STAGE` - Transformation stage before loading to final table
3. `FOREX_RATES` - Final table for storing the processed data

## Airflow DAG
The Airflow DAG consists of two main tasks:

![Airflow DAG](config/icon/p2.png)

1. `fetch_forex_rates` - Python operator that fetches data from Alpha Vantage API
2. `process_and_store_data` - Python operator that processes and stores data in Snowflake

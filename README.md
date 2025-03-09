## Real-time Data Pipeline Project

### Project Overview
This project is a **Real-time & Historical Data Pipeline** designed to fetch currency exchange rates from the [**Alpha Vantage API**](https://www.alphavantage.co), transform the data into structured formats, and prepare it for analysis. The pipeline follows the ETL (Extract, Transform, Load) process.

### Features
- Real-time forex data fetching
- Historical forex data collection
- Data transformation and cleaning
- CSV export for analysis
- Error handling and logging
- Support for multiple currency pairs (EUR/USD, USD/EGP, EUR/EGP)

### Project Structure
```plaintext
Real-time Data Pipeline/
├── data_ingestion/              # Scripts to fetch real-time & historical data
│   ├── fetch_data.py           # Real-time data fetcher (current rates)
│   └── fetch_historical_data.py # Historical data fetcher (daily rates)
├── data_storage/               # Scripts for data storage operations
│   └── save_to_csv.py         # Convert JSON data to CSV format
├── data/                      # Raw data storage (JSON files)
│   ├── realtime_*.json       # Real-time forex data
│   └── historical_*.json     # Historical forex data
├── processed_data/            # Transformed data storage (CSV files)
│   ├── realtime_*.csv       # Processed real-time data
│   └── historical_*.csv     # Processed historical data
├── config/                    # Configuration files
│   └── config.json          # API keys and currency pairs config
└── logs/                     # Log files
    ├── forex_data.log      # Real-time data logs
    └── historical_forex.log # Historical data logs
```

### Setup and Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure API settings in `config/config.json`:
```json
{
    "forex_api_url": "https://www.alphavantage.co/query",
    "forex_api_key": "YOUR_API_KEY",
    "currency_pairs": [
        "EUR/USD",
        "USD/EGP",
        "EUR/EGP"
    ]
}
```

### Usage

1. Fetch real-time forex data:
```bash
python data_ingestion/fetch_data.py
```

2. Fetch historical forex data:
```bash
python data_ingestion/fetch_historical_data.py
```

3. Process and convert data to CSV:
```bash
python data_storage/save_to_csv.py
```

### Data Files

- **Real-time Data**: `data/realtime_[FROM]_[TO].json`
- **Historical Data**: `data/historical_[FROM]_[TO].json`
- **Processed CSV**: `processed_data/[realtime|historical]_[FROM]_[TO].csv`

### Logging

The system maintains detailed logs:
- `forex_data.log`: Real-time data operations
- `historical_forex.log`: Historical data operations
- `forex_processing.log`: Data processing operations

### Error Handling

The pipeline includes comprehensive error handling for:
- API connection issues
- Data validation
- File operations
- Rate limiting

### Rate Limits

- Real-time data: 5 API calls per minute
- Historical data: 15 seconds between requests

## Real-time Data Pipeline Project

###  Project Overview
This project is a **Real-time & Historical Data Pipeline** designed to fetch currency exchange rates from the **Alpha Vantage API**, transform the data into structured formats, and prepare it for analysis. The pipeline follows the ETL (Extract, Transform, Load) process.

---

##  Structure
```plaintext
Real-time Data Pipeline/
├── data_ingestion/              # Scripts to fetch real-time & historical data
│   ├─ fetch_data.py             # Real-time data fetcher (current rates)
│   └─ fetch_historical_data.py  # Historical data fetcher (daily rates)
├── data_transformation/          # Scripts to clean & transform data
│   └─ transform_data.py         # Transformation logic for historical data
├── data/                         # Raw data storage (fetched JSON files)
├── processed_data/               # Transformed data storage (cleaned CSV files)
├── config/                       # Configuration files (API key, currency pairs)
│   └─ config.json


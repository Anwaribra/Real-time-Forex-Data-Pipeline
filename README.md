## Real-time Data Pipeline Project

### Project Overview
This project is a **Real-time & Historical Data Pipeline** designed to fetch currency exchange rates from the [**Alpha Vantage API**](https://www.alphavantage.co), process through Kafka streaming, and prepare it for analysis. The pipeline follows the ETL (Extract, Transform, Load) process with real-time streaming capabilities.

### Features
- Real-time forex data fetching
- Historical forex data collection
- Kafka streaming integration
- Data transformation and cleaning
- CSV export for analysis
- Error handling and logging
- Support for multiple currency pairs (EUR/USD, USD/EGP, EUR/EGP)
- Docker containerization
- Apache Airflow integration

### Project Structure
```plaintext
Real-time Data Pipeline/
├── data_ingestion/              # Scripts to fetch real-time & historical data
│   ├── fetch_data.py           # Real-time data fetcher (current rates)
│   └── fetch_historical_data.py # Historical data fetcher (daily rates)
├── data_storage/               # Scripts for data storage operations
│   └── save_to_csv.py         # Convert JSON data to CSV format
├── data/                      # Raw data storage (JSON files)
├── processed_data/            # Transformed data storage (CSV files)
├── config/                    # Configuration files
│   └── config.json          # API keys and currency pairs config
├── logs/                     # Log files
├── docker-compose.yaml       # Docker services configuration
├── Dockerfile               # Python application container configuration
└── requirements.txt         # Python dependencies
```

### Technologies Used
- Python 3.9
- Apache Kafka
- Apache Airflow
- Docker & Docker Compose
- PostgreSQL (for Airflow metadata)

### Setup and Installation

1. Clone the repository
```bash
git clone <repository-url>
cd Real-time-Data-Pipeline
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env and add your API key
```

3. Configure settings:
```bash
cp config/config.json.example config/config.json
```

4. Start the Docker services:
```bash
docker-compose up --build
```

### Docker Services
The application runs the following services:
- **Kafka**: Message streaming platform
- **Zookeeper**: Required for Kafka coordination
- **Airflow Webserver**: Workflow management UI
- **Airflow Scheduler**: Task scheduling
- **PostgreSQL**: Metadata database
- **Python App**: Main pipeline application

### Kafka Integration

1. Topics:
- `forex_data`: Real-time currency exchange rates

2. Producers:
- Real-time data fetcher
- Historical data fetcher

3. Consumers:
- Data transformation service
- Data storage service

### Usage

1. Access services:
- Airflow UI: http://localhost:8081
- Kafka: localhost:9092

2. Monitor Kafka topics:
```bash
# Enter Kafka container
docker-compose exec kafka bash

# List topics
kafka-topics --list --bootstrap-server kafka:29092

# Monitor messages
kafka-console-consumer --bootstrap-server kafka:29092 --topic forex_data --from-beginning
```

3. View logs:
```bash
docker-compose logs -f app
```

### Data Flow
1. Data Ingestion:
   - Fetch forex data from Alpha Vantage API
   - Publish to Kafka topic

2. Data Processing:
   - Consume messages from Kafka
   - Transform and validate data
   - Store in processed format

3. Data Storage:
   - Save processed data to CSV files
   - Generate analysis reports

### Error Handling

The pipeline includes comprehensive error handling for:
- API connection issues
- Kafka connectivity
- Data validation
- File operations
- Rate limiting

### Rate Limits
- Real-time data: 5 API calls per minute
- Historical data: 15 seconds between requests
- Kafka retry configuration included

### Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

### Troubleshooting

1. Kafka Connection Issues:
```bash
# Check Kafka logs
docker-compose logs kafka

# Verify network connectivity
docker network ls
docker network inspect app-network
```

2. Application Issues:
```bash
# Check application logs
docker-compose logs app

# Access container shell
docker-compose exec app bash
```

### Maintenance

1. Clean up Docker resources:
```bash
# Stop services
docker-compose down

# Remove volumes
docker-compose down -v

# Remove all containers and images
docker-compose down --rmi all
```

2. Update dependencies:
```bash
# Update requirements.txt
pip freeze > requirements.txt

# Rebuild containers
docker-compose up --build
```


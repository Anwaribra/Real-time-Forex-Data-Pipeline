import requests
import json
import time
from datetime import datetime
import logging
from pathlib import Path
from kafka import KafkaProducer
from json import dumps
from data_warehouse.snowflake_connector import SnowflakeConnector

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def load_config():
    """Load configuration from config file."""
    try:
        config_path = Path(__file__).parent.parent / "config" / "config.json"
        with open(config_path, "r") as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Error loading config: {str(e)}")
        raise

def fetch_historical_data():
    """Main function to fetch and save historical forex data."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = load_config()
        api_url = config["forex_api_url"]
        api_key = config["forex_api_key"]
        currency_pairs = config["currency_pairs"]
        
        # Setup Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=config["kafka_bootstrap_servers"],
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )
        
        # Process each currency pair
        for pair in currency_pairs:
            from_currency, to_currency = pair.split('/')
            
            # Fetch data for the currency pair
            data = fetch_historical_data_for_pair(api_url, api_key, from_currency, to_currency)
            if data:
                # Send data to Kafka
                producer.send(config["kafka_topic"], value=data)
                logger.info(f"Sent {pair} data to Kafka")
            time.sleep(15)  # Respect API rate limits
            
        logger.info("Historical data fetch completed")
        
    except Exception as e:
        logger.error(f"Fatal error in fetch_historical_data: {str(e)}")
        raise
    finally:
        producer.close()

def fetch_historical_data_for_pair(api_url, api_key, from_currency, to_currency, outputsize="full"):
    """Fetch historical forex data from API for a specific currency pair."""
    logger = logging.getLogger(__name__)
    
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_currency,
        "to_symbol": to_currency,
        "apikey": api_key,
        "outputsize": outputsize
    }
    
    try:
        logger.info(f"Fetching historical data for {from_currency}/{to_currency}")
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if "Error Message" in data:
            logger.error(f"API Error for {from_currency}/{to_currency}: {data['Error Message']}")
            return None
            
        if "Time Series FX (Daily)" not in data:
            logger.error(f"No historical data available for {from_currency}/{to_currency}")
            return None
            
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {from_currency}/{to_currency}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error for {from_currency}/{to_currency}: {str(e)}")
        return None

def save_historical_data_to_json(pair, data):
    """Save historical data to JSON file."""
    logger = logging.getLogger(__name__)
    
    try:
        # Ensure data directory exists
        data_dir = Path(__file__).parent.parent / "data"
        data_dir.mkdir(exist_ok=True)
        
        from_currency, to_currency = pair.split('/')
        filename = data_dir / f"historical_{from_currency}_{to_currency}.json"
        
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)
        logger.info(f"Saved historical data to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving data for {pair}: {str(e)}")

def fetch_forex_data():
    """
    Fetch currency data and send it to Kafka
    """
    logger = logging.getLogger(__name__)
    setup_logging()
    
    # Setup Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    
    pairs = ['EUR_EGP', 'USD_EGP', 'EUR_USD']
    base_url = "https://www.alphavantage.co/query"  
    
    try:
        for pair in pairs:
            from_currency, to_currency = pair.split('_')
            params = {
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": from_currency,
                "to_currency": to_currency,
                "apikey": "YOUR_API_KEY"  # Replace with your actual API key
            }
            response = requests.get(base_url, params=params)
            data = response.json()
            
            # Send data to Kafka
            producer.send('forex_data', value=data)
            logger.info(f"Sent {pair} data to Kafka")
            
    except Exception as e:
        logger.error(f"General error: {str(e)}")
    finally:
        producer.close()

def fetch_and_store_historical_data(currency_pair, start_date, end_date, api_key):
    """
    Import and store historical data for a currency pair
    """
    # After importing data and converting to DataFrame
    
    # Store data in Snowflake
    try:
        snowflake = SnowflakeConnector()
        table_name = f"historical_rates_{currency_pair.replace('/', '_')}"
        snowflake.load_dataframe_to_table(df, table_name)
        snowflake.close()
    except Exception as e:
        print(f"Error storing data in Snowflake: {e}")

if __name__ == "__main__":
    fetch_historical_data()
import requests
import json
import time
from datetime import datetime, timedelta
import logging
from pathlib import Path
import os

def setup_logging():
    """Configure logging settings."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('historical_forex.log'),
            logging.StreamHandler()
        ]
    )

def load_config():
    """Load configuration from config file."""
    try:
        config_path = Path("config/config.json")
        with open(config_path, "r") as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Error loading config: {str(e)}")
        raise

def fetch_historical_data(from_currency, to_currency, outputsize="full"):
    """Fetch historical forex data from API."""
    logger = logging.getLogger(__name__)
    
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_currency,
        "to_symbol": to_currency,
        "apikey": API_KEY,
        "outputsize": outputsize
    }
    
    try:
        logger.info(f"Fetching historical data for {from_currency}/{to_currency}")
        response = requests.get(API_URL, params=params, timeout=30)
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
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True)
        
        from_currency, to_currency = pair.split('/')
        filename = data_dir / f"historical_{from_currency}_{to_currency}.json"
        
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)
        logger.info(f"Saved historical data to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving data for {pair}: {str(e)}")

def main():
    """Main function to fetch and save historical forex data."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        global API_URL, API_KEY, CURRENCY_PAIRS
        config = load_config()
        API_URL = config["forex_api_url"]
        API_KEY = config["forex_api_key"]
        CURRENCY_PAIRS = config["currency_pairs"]
        
        # Process each currency pair
        for pair in CURRENCY_PAIRS:
            from_currency, to_currency = pair.split('/')
            
            # Fetch data for all currency pairs
            data = fetch_historical_data(from_currency, to_currency)
            if data:
                save_historical_data_to_json(pair, data)
            time.sleep(15)  # Respect API rate limits
            
        logger.info("Historical data fetch completed")
        
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()

import requests
import json
import os
import logging
import time
from typing import Dict, List
from pathlib import Path

def load_config() -> Dict:
    """Load and validate configuration file."""
    try:
        config_path = Path(__file__).parent.parent / 'config' / 'config.json'
        with open(config_path, 'r') as file:
            config = json.load(file)
            
        required_fields = ['forex_api_url', 'forex_api_key', 'currency_pairs']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")
                
        return config
    except FileNotFoundError:
        raise FileNotFoundError("Config file not found. Please ensure config.json exists.")
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON in config file.")

def setup_logging() -> None:
    """Configure logging settings."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('forex_data.log'),
            logging.StreamHandler()
        ]
    )

def fetch_forex_data() -> None:
    """Fetch and save forex data for configured currency pairs."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        config = load_config()
        base_url = config['forex_api_url']
        api_key = config['forex_api_key']
        currency_pairs = config['currency_pairs']

        # Ensure 'data/' directory exists
        data_dir = Path(__file__).parent.parent / 'data'
        data_dir.mkdir(exist_ok=True)

        for pair in currency_pairs:
            try:
                from_currency, to_currency = pair.split('/')
                params = {
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": from_currency,
                    "to_currency": to_currency,
                    "apikey": api_key
                }

                # Add delay to respect rate limits
                time.sleep(1)  # Adjust based on API rate limits

                response = requests.get(base_url, params=params, timeout=10)
                response.raise_for_status()

                data = response.json()
                logger.info(f"Data fetched for {pair}")
                
                # Save data to JSON file
                filename = f"realtime_{from_currency}_{to_currency}.json"
                filepath = data_dir / filename
                
                with open(filepath, "w") as file:
                    json.dump(data, file, indent=4)

                logger.info(f"Data saved to {filepath}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for {pair}: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing {pair}: {str(e)}")
                
            # Add delay between requests
            time.sleep(0.5)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == '__main__':
    fetch_forex_data()

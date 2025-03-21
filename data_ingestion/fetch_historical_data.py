import requests
import json
import time
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path


LOG_FILE = "historical_forex.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def load_config():
    """Load configuration from JSON config file."""
    config_path = Path("config/config.json")
    try:
        with open(config_path, "r") as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise


def fetch_historical_data(api_url, api_key, from_currency, to_currency, outputsize="full"):
    """Fetch historical forex data from API."""
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_currency,
        "to_symbol": to_currency,
        "apikey": api_key,
        "outputsize": outputsize,
    }

    try:
        logger.info(f"Fetching historical data for {from_currency}/{to_currency}...")
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            logger.error(f"API Error: {data['Error Message']}")
            return None
        if "Time Series FX (Daily)" not in data:
            logger.error(f"No historical data available for {from_currency}/{to_currency}")
            return None

        return data["Time Series FX (Daily)"]

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None


def save_data_to_csv(data_dir, pair, data):
    """Save historical forex data to CSV file."""
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        from_currency, to_currency = pair.split('/')
        file_path = data_dir / f"historical_{from_currency}_{to_currency}.csv"

        
        df = pd.DataFrame.from_dict(data, orient="index")
        df.reset_index(inplace=True)
        df.rename(columns={"index": "date"}, inplace=True)

       
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        
        df.to_csv(file_path, index=False)
        logger.info(f"Saved historical data to {file_path}")

    except Exception as e:
        logger.error(f"Error saving data for {pair}: {e}")


def main():
    """Main function to fetch and save historical forex data."""
    config = load_config()
    api_url = config["forex_api_url"]
    api_key = config["forex_api_key"]
    currency_pairs = config["currency_pairs"]
    data_dir = Path("data")

    requests_per_minute = config.get("api_rate_limits", {}).get("requests_per_minute", 5)
    delay_between_requests = 60 / requests_per_minute  
    for pair in currency_pairs:
        from_currency, to_currency = pair.split('/')
        data = fetch_historical_data(api_url, api_key, from_currency, to_currency)

        if data:
            save_data_to_csv(data_dir, pair, data)

        logger.info(f"Waiting {delay_between_requests:.1f} seconds before next request...")
        time.sleep(delay_between_requests)

    logger.info("Historical data fetch completed!")

if __name__ == "__main__":
    main()

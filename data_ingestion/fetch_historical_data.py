import requests
import json
import time
from datetime import datetime, timedelta
import logging


with open("config/config.json", "r") as file:
    config = json.load(file)

API_URL = config["forex_api_url"]
API_KEY = config["forex_api_key"]
CURRENCY_PAIRS = config["currency_pairs"]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_historical_data(from_currency, to_currency, outputsize="full"):
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_currency,
        "to_symbol": to_currency,
        "apikey": API_KEY,
        "outputsize": outputsize
    }
    response = requests.get(API_URL, params=params)
    if response.status_code != 200:
        logging.error(f"Failed to fetch data for {from_currency}/{to_currency}: {response.status_code}")
        return None
    return response.json()

def save_historical_data_to_json(pair, data):
    from_currency, to_currency = pair.split('/')
    filename = f"historical_{from_currency}_{to_currency}.json"
    with open(f"data/{filename}", "w") as file:
        json.dump(data, file, indent=4)
    logging.info(f"Saved historical data to data/{filename}")

def main():
    for pair in CURRENCY_PAIRS:
        from_currency, to_currency = pair.split('/')
        logging.info(f"Fetching historical data for {from_currency}/{to_currency}")
        data = fetch_historical_data(from_currency, to_currency)
        if data:
            save_historical_data_to_json(pair, data)
        time.sleep(15)  # Alpha Vantage free tier rate limit

if __name__ == "__main__":
    main()

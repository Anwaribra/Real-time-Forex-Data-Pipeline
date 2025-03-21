import requests
import json
import pandas as pd
from datetime import datetime
import logging
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("forex_data.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

def load_config() -> dict:
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    with open(config_path, "r") as file:
        return json.load(file)

def save_data_files(df):
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    csv_path = data_dir / f"forex_rates_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"Data saved to {csv_path}")
    
    return csv_path

def fetch_forex_data() -> pd.DataFrame:
    logger.info("Starting forex data fetch")
    
    try:
        config = load_config()
        logger.info(f"Config loaded, found {len(config['currency_pairs'])} currency pairs")
        results = []

        for pair in config["currency_pairs"]:
            try:
                from_currency, to_currency = pair.split("/")
                
                params = {
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": from_currency,
                    "to_currency": to_currency,
                    "apikey": config["forex_api_key"],
                }

                response = requests.get(config["forex_api_url"], params=params, timeout=15)
                
                data = response.json()
                if "Error Message" in data:
                    logger.error(f"API error: {data['Error Message']}")
                    continue
                    
                if "Note" in data:
                    logger.warning(f"API rate limit reached. Pausing...")
                    time.sleep(60)
                    continue

                if "Realtime Currency Exchange Rate" in data:
                    rate_data = data["Realtime Currency Exchange Rate"]
                    exchange_rate = float(rate_data["5. Exchange Rate"])
                    
                    results.append({
                        "from_currency": from_currency,
                        "to_currency": to_currency,
                        "exchange_rate": exchange_rate,
                        "timestamp": datetime.now().isoformat(),
                    })
                else:
                    logger.error(f"Unexpected response format for {pair}: {data.keys()}")

                time.sleep(3)

            except Exception as e:
                logger.error(f"Failed to fetch data for {pair}: {str(e)}")

        if results:
            df = pd.DataFrame(results)
            save_data_files(df)
            return df
        
        logger.warning("No data was fetched from the API")
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Fatal error in fetch_forex_data: {str(e)}")
        return pd.DataFrame()

if __name__ == "__main__":
    result = fetch_forex_data()
    print(f"Results shape: {result.shape}")
    print(result)

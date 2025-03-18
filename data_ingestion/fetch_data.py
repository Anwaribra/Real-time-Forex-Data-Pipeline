import requests
import json
import pandas as pd
from datetime import datetime
import logging
import time
from pathlib import Path

def load_config() -> dict:
    """Load configuration file."""
    config_path = Path(__file__).parent.parent / 'config' / 'config.json'
    with open(config_path, 'r') as file:
        return json.load(file)

def fetch_forex_data() -> pd.DataFrame:
    """Fetch current forex data for configured currency pairs."""
    # Setup better logging with both file and console output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('forex_data.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting forex data fetch")
    
    try:
        config = load_config()
        logger.info(f"Config loaded, found {len(config['currency_pairs'])} currency pairs")
        results = []

        for pair in config['currency_pairs']:
            try:
                from_currency, to_currency = pair.split('/')
                logger.info(f"Fetching data for {from_currency}/{to_currency}")
                
                params = {
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": from_currency,
                    "to_currency": to_currency,
                    "apikey": config['forex_api_key']
                }

                # Debug the API request URL and parameters
                api_url = config['forex_api_url']
                logger.info(f"API URL: {api_url}")
                logger.info(f"Params: {params}")
                
                # Make request with better error handling
                response = requests.get(api_url, params=params, timeout=15)
                
                # Log the response status and content length
                logger.info(f"Response status: {response.status_code}, Content length: {len(response.text)}")
                
                # Check for API error message
                data = response.json()
                if "Error Message" in data:
                    logger.error(f"API error: {data['Error Message']}")
                    continue
                    
                if "Note" in data:
                    logger.warning(f"API note: {data['Note']}")

                if 'Realtime Currency Exchange Rate' in data:
                    rate_data = data['Realtime Currency Exchange Rate']
                    exchange_rate = float(rate_data['5. Exchange Rate'])
                    
                    logger.info(f"Successfully fetched rate for {pair}: {exchange_rate}")
                    
                    results.append({
                        'from_currency': from_currency,
                        'to_currency': to_currency,
                        'exchange_rate': exchange_rate,
                        'timestamp': datetime.now().isoformat()
                    })
                else:
                    logger.error(f"Unexpected response format for {pair}: {data.keys()}")
                
                # Respect API rate limits
                logger.info("Waiting for rate limit...")
                time.sleep(3)  # Increased sleep time to be safe

            except Exception as e:
                logger.error(f"Failed to fetch data for {pair}: {str(e)}")
                logger.exception("Exception details:")

        if results:
            logger.info(f"Successfully fetched data for {len(results)} currency pairs")
            df = pd.DataFrame(results)
            
            # Save to CSV with timestamp
            data_dir = Path(__file__).parent.parent / 'data'
            data_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            csv_path = data_dir / f'forex_rates_{timestamp}.csv'
            df.to_csv(csv_path, index=False)
            logger.info(f"Data saved to {csv_path}")
            
            # Also save a JSON version
            json_path = data_dir / f'forex_rates_{timestamp}.json'
            results_dict = {
                "timestamp": datetime.now().isoformat(),
                "rates": {}
            }
            
            for row in results:
                key = f"{row['from_currency']}_{row['to_currency']}"
                results_dict["rates"][key] = row['exchange_rate']
            
            with open(json_path, 'w') as f:
                json.dump(results_dict, f, indent=2)
            logger.info(f"JSON data saved to {json_path}")
            
            return df
        else:
            logger.warning("No data was fetched from the API")
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Fatal error in fetch_forex_data: {str(e)}")
        logger.exception("Exception details:")
        return pd.DataFrame()

if __name__ == '__main__':
    result = fetch_forex_data()
    print(f"Results shape: {result.shape}")
    print(result)
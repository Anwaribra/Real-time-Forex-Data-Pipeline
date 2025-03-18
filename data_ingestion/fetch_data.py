import requests
import json
import pandas as pd
from datetime import datetime
import logging
import time
from pathlib import Path
import os

# Setup logging once at module level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('forex_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config() -> dict:
    """Load configuration file."""
    config_path = Path(__file__).parent.parent / 'config' / 'config.json'
    with open(config_path, 'r') as file:
        return json.load(file)

def save_data_files(df, is_mock=False):
    """Save dataframe to CSV and JSON files - reduces code duplication"""
    # Save to CSV with timestamp
    data_dir = Path(__file__).parent.parent / 'data'
    data_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    prefix = "mock_" if is_mock else ""
    csv_path = data_dir / f'{prefix}forex_rates_{timestamp}.csv'
    df.to_csv(csv_path, index=False)
    logger.info(f"Data saved to {csv_path}")
    
    # Also save as JSON
    json_path = data_dir / f'{prefix}forex_rates_{timestamp}.json'
    results_dict = {
        "timestamp": datetime.now().isoformat(),
        "rates": {}
    }
    
    for _, row in df.iterrows():
        key = f"{row['from_currency']}_{row['to_currency']}"
        results_dict["rates"][key] = row['exchange_rate']
    
    with open(json_path, 'w') as f:
        json.dump(results_dict, f, indent=2)
    logger.info(f"JSON data saved to {json_path}")
    
    return csv_path

def generate_mock_data() -> pd.DataFrame:
    """Generate mock forex data when API is unavailable or rate-limited"""
    logger.info("Generating mock data due to API limitations")
    
    config = load_config()
    currency_pairs = config.get('currency_pairs', ['EUR/USD', 'USD/JPY', 'GBP/USD'])
    
    # Fixed mock exchange rates
    mock_rates = {
        'EUR/USD': 1.0922,
        'USD/JPY': 149.12,
        'GBP/USD': 1.2734,
        'USD/EGP': 30.85,
        'EUR/EGP': 33.70
    }
    
    results = []
    
    for pair in currency_pairs:
        if pair in mock_rates:
            from_currency, to_currency = pair.split('/')
            results.append({
                'from_currency': from_currency,
                'to_currency': to_currency,
                'exchange_rate': mock_rates[pair],
                'last_refreshed': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': datetime.now().isoformat()
            })
    
    df = pd.DataFrame(results)
    save_data_files(df, is_mock=True)
    return df

def fetch_forex_data(use_mock=False) -> pd.DataFrame:
    """Fetch current forex data for configured currency pairs."""
    logger.info("Starting forex data fetch")
    
    # If mock data is requested, use that instead of API
    if use_mock:
        logger.info("Using mock data as requested")
        return generate_mock_data()
    
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
                    # API rate limit reached, switch to mock data
                    if "API call frequency" in data["Note"]:
                        logger.warning("API rate limit reached, switching to mock data")
                        return generate_mock_data()

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
                    # If we're receiving Information instead of Realtime Currency, likely hit limit
                    if "Information" in data:
                        logger.warning("API limit message detected, switching to mock data")
                        return generate_mock_data()
                
                # Respect API rate limits
                logger.info("Waiting for rate limit...")
                time.sleep(3)  # Increased sleep time to be safe

            except Exception as e:
                logger.error(f"Failed to fetch data for {pair}: {str(e)}")
                logger.exception("Exception details:")

        if results:
            logger.info(f"Successfully fetched data for {len(results)} currency pairs")
            df = pd.DataFrame(results)
            save_data_files(df)
            return df
        else:
            logger.warning("No data was fetched from the API, using mock data")
            return generate_mock_data()
        
    except Exception as e:
        logger.error(f"Fatal error in fetch_forex_data: {str(e)}")
        logger.exception("Exception details:")
        # Fall back to mock data in case of any error
        logger.info("Falling back to mock data due to error")
        return generate_mock_data()

if __name__ == '__main__':
    # Add command line argument to choose between real and mock data
    import argparse
    parser = argparse.ArgumentParser(description='Fetch forex data')
    parser.add_argument('--mock', action='store_true', help='Use mock data instead of real API')
    args = parser.parse_args()
    
    result = fetch_forex_data(use_mock=args.mock)
    print(f"Results shape: {result.shape}")
    print(result)
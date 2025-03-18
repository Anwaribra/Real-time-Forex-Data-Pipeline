import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
from pathlib import Path
import os
import sys

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_ingestion.fetch_data import load_config  # Reuse the config loading

def _is_date_in_range(date_str: str, start_date: datetime, end_date: datetime) -> bool:
    """Check if date is within the specified range."""
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return start_date <= date_obj <= end_date

def _make_api_request(url: str, params: dict, max_retries: int = 3) -> dict:
    """Make API request with retries."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return {}  # Should not reach here due to the raise, but added for completeness

def fetch_historical_data(days: int = 7, data_dir: str = None) -> pd.DataFrame:
    """Fetch historical forex data for the last N days."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting historical data fetch for the past {days} days")
    
    config = load_config()
    results = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    total_pairs = len(config['currency_pairs'])
    for i, pair in enumerate(config['currency_pairs'], 1):
        logger.info(f"Processing {pair} ({i}/{total_pairs})")
        try:
            from_currency, to_currency = pair.split('/')
            params = {
                "function": "FX_DAILY",
                "from_symbol": from_currency,
                "to_symbol": to_currency,
                "apikey": config['forex_api_key'],
                "outputsize": "full"
            }
            
            data = _make_api_request(config['forex_api_url'], params)
            
            if 'Time Series FX (Daily)' in data:
                daily_data = data['Time Series FX (Daily)']
                
                for date, rates in daily_data.items():
                    if _is_date_in_range(date, start_date, end_date):
                        results.append({
                            'date': date,
                            'from_currency': from_currency,
                            'to_currency': to_currency,
                            'close': float(rates['4. close'])
                        })
            
            time.sleep(12)  # API rate limit
            
        except Exception as e:
            logger.error(f"Error with {pair}: {str(e)}")
    
    if results:
        if data_dir is None:
            data_dir = Path(__file__).parent.parent / 'data'
        else:
            data_dir = Path(data_dir)
        data_dir.mkdir(exist_ok=True)
        
        today = datetime.now().strftime("%Y%m%d")
        df = pd.DataFrame(results)
        df.to_csv(data_dir / f'historical_rates_{today}.csv', index=False)
        
        # Create a single consolidated JSON
        consolidated = {"timestamp": datetime.now().isoformat(), "rates": {}}
        for date in sorted(df['date'].unique()):
            consolidated["rates"][date] = {}
            for pair in config['currency_pairs']:
                from_curr, to_curr = pair.split('/')
                key = f"{from_curr}_{to_curr}"
                match = df[(df['date'] == date) & 
                           (df['from_currency'] == from_curr) & 
                           (df['to_currency'] == to_curr)]
                if not match.empty:
                    consolidated["rates"][date][key] = float(match.iloc[0]['close'])
        
        with open(data_dir / f'historical_rates_{today}.json', 'w') as f:
            json.dump(consolidated, f, indent=2)
        
        return df
    
    return pd.DataFrame()

if __name__ == '__main__':
    fetch_historical_data(days=7)
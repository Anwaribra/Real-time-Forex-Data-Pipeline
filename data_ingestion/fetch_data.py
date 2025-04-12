import requests
import pandas as pd
import logging
from datetime import datetime
import time
from pathlib import Path
import json
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ForexDataFetcher:
    def __init__(self, config_path="config/config.json"):
        """Initialize with configuration"""
        self.config = self._load_config(config_path)
        self.api_key = self.config["forex_api_key"]
        self.api_url = self.config["forex_api_url"]
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
    
    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def fetch_forex_data(self, from_currency: str, to_currency: str) -> Optional[pd.DataFrame]:
        """Fetch forex data from Alpha Vantage API"""
        try:
            # Special handling for EUR/EGP
            if from_currency == "EUR" and to_currency == "EGP":
                return self._calculate_eur_egp()
            
            time.sleep(12)
            
            params = {
                "function": "FX_DAILY",
                "from_symbol": from_currency,
                "to_symbol": to_currency,
                "apikey": self.api_key,
                "outputsize": "full"
            }
            
            logger.info(f"Fetching {from_currency}/{to_currency} data...")
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
                
            if "Note" in data:
                logger.warning(f"API limit warning: {data['Note']}")
                time.sleep(60)  
                return self.fetch_forex_data(from_currency, to_currency)
            
            if "Time Series FX (Daily)" not in data:
                logger.error("No data found in API response")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame.from_dict(data["Time Series FX (Daily)"], orient="index")
            df.index = pd.to_datetime(df.index)
            df.columns = ["Open", "High", "Low", "Close"]
            df = df.astype(float).round(4)
            
            logger.info(f"Successfully fetched {len(df)} records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return None
    
    def _calculate_eur_egp(self) -> Optional[pd.DataFrame]:
        """Calculate EUR/EGP using EUR/USD and USD/EGP rates"""
        try:
            # Get EUR/USD data
            eur_usd = self.fetch_forex_data("EUR", "USD")
            if eur_usd is None:
                return None
            
            # Get USD/EGP data
            usd_egp = self.fetch_forex_data("USD", "EGP")
            if usd_egp is None:
                return None
            
            # Calculate cross rates
            eur_egp = pd.DataFrame(index=eur_usd.index)
            for col in ["Open", "High", "Low", "Close"]:
                eur_egp[col] = (eur_usd[col] * usd_egp[col]).round(4)
            
            return eur_egp
            
        except Exception as e:
            logger.error(f"Error calculating EUR/EGP: {str(e)}")
            return None
    
    def save_data(self, df: pd.DataFrame, pair: str) -> None:
        """Save data to CSV file"""
        try:
            filename = f"historical_{pair.replace('/', '_')}.csv"
            file_path = self.data_dir / filename
            
            # Sort by date descending
            df = df.sort_index(ascending=False)
            
            # Save with proper formatting
            df.to_csv(file_path, float_format='%.4f')
            logger.info(f"Saved {len(df)} records to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
    
    def update_all_pairs(self) -> None:
        """Fetch and save data for all currency pairs"""
        pairs = [
            ("USD", "EGP"),
            ("EUR", "USD"),
            ("EUR", "EGP")
        ]
        
        for from_curr, to_curr in pairs:
            if df := self.fetch_forex_data(from_curr, to_curr):
                self.save_data(df, f"{from_curr}_{to_curr}")

if __name__ == "__main__":
    try:
        fetcher = ForexDataFetcher()
        fetcher.update_all_pairs()
        logger.info("Data update completed successfully")
    except Exception as e:
        logger.error(f"Fatal error: {e}")

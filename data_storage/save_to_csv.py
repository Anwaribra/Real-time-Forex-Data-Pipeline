import os
import pandas as pd
import json
import logging
from typing import Dict, Optional
from pathlib import Path

def setup_logging() -> None:
    """Configure logging settings."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('forex_processing.log'),
            logging.StreamHandler()
        ]
    )

def validate_realtime_data(data: Dict) -> bool:
    """Validate the structure of realtime forex data."""
    required_fields = [
        "1. From_Currency Code",
        "3. To_Currency Code",
        "5. Exchange Rate",
        "6. Last Refreshed",
        "8. Bid Price",
        "9. Ask Price"
    ]
    
    if "Realtime Currency Exchange Rate" not in data:
        return False
        
    exchange_data = data["Realtime Currency Exchange Rate"]
    return all(field in exchange_data for field in required_fields)

def process_realtime_data(exchange_data: Dict) -> Dict:
    """Extract and structure realtime exchange rate data."""
    return {
        "From_Currency": exchange_data["1. From_Currency Code"],
        "To_Currency": exchange_data["3. To_Currency Code"],
        "Exchange_Rate": float(exchange_data["5. Exchange Rate"]),
        "Last_Refreshed": exchange_data["6. Last Refreshed"],
        "Bid_Price": float(exchange_data["8. Bid Price"]),
        "Ask_Price": float(exchange_data["9. Ask Price"])
    }

def save_realtime_to_csv(data: Dict, pair: str, output_dir: Optional[Path] = None) -> None:
    """
    Save real-time exchange rate data to CSV.
    
    Args:
        data: Dictionary containing forex data
        pair: Currency pair (e.g., 'EUR/USD')
        output_dir: Directory to save the CSV file (default: 'processed_data')
    """
    logger = logging.getLogger(__name__)
    
    try:
        if not validate_realtime_data(data):
            logger.warning(f"Invalid or missing real-time data for {pair}")
            return
        
        
        output_dir = output_dir or Path("processed_data")
        output_dir.mkdir(exist_ok=True)
        
        # Process and save data
        exchange_data = data["Realtime Currency Exchange Rate"]
        structured_data = process_realtime_data(exchange_data)
        
        df = pd.DataFrame([structured_data])
        filename = output_dir / f"realtime_{pair.replace('/', '_')}.csv"
        
        
        df.to_csv(filename, mode="a", index=False, header=not filename.exists())
        logger.info(f"Real-time data saved to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving real-time data for {pair}: {str(e)}")
        raise

def validate_historical_data(data: Dict) -> bool:
    """Validate historical data structure."""
    if "Time Series FX (Daily)" not in data:
        return False
        
    # Check if we have at least one data point
    time_series = data["Time Series FX (Daily)"]
    if not time_series:
        return False
        
    # Validate data structure
    sample_point = next(iter(time_series.values()))
    required_fields = ["1. open", "2. high", "3. low", "4. close"]
    return all(field in sample_point for field in required_fields)

def save_historical_to_csv(data: Dict, pair: str, output_dir: Optional[Path] = None) -> None:
    """
    Save historical exchange rate data to CSV.
    
    Args:
        data: Dictionary containing historical forex data
        pair: Currency pair (e.g., 'EUR/USD')
        output_dir: Directory to save the CSV file (default: 'processed_data')
    """
    logger = logging.getLogger(__name__)
    
    try:
        if not validate_historical_data(data):
            logger.warning(f"Invalid or missing historical data for {pair}")
            return
        
        # Set up output directory
        output_dir = output_dir or Path("processed_data")
        output_dir.mkdir(exist_ok=True)
        
        # Convert nested JSON to DataFrame
        df = pd.DataFrame.from_dict(data["Time Series FX (Daily)"], orient="index")
        df.index.name = "Date"
        
        # Rename columns and convert to numeric
        df.rename(columns={
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close"
        }, inplace=True)
        
        for col in ["Open", "High", "Low", "Close"]:
            df[col] = pd.to_numeric(df[col])
        
        filename = output_dir / f"historical_{pair.replace('/', '_')}.csv"
        df.to_csv(filename)
        logger.info(f"Historical data saved to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving historical data for {pair}: {str(e)}")
        raise

def process_all_forex_data(data_dir: Optional[Path] = None) -> None:
    """Process all forex data files in the data directory."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    data_dir = data_dir or Path("data")
    
    try:
        for file in data_dir.glob("realtime_*.json"):
            pair = file.stem.replace("realtime_", "").replace("_", "/")
            with open(file, "r") as f:
                data = json.load(f)
                save_realtime_to_csv(data, pair)
                
        for file in data_dir.glob("historical_*.json"):
            pair = file.stem.replace("historical_", "").replace("_", "/")
            with open(file, "r") as f:
                data = json.load(f)
                save_historical_to_csv(data, pair)
                
    except Exception as e:
        logger.error(f"Error processing forex data: {str(e)}")
        raise

def process_historical_data(data_dir: Optional[Path] = None) -> None:
    """Process all historical forex data files."""
    logger = logging.getLogger(__name__)
    output_dir = Path("processed_data")
    output_dir.mkdir(exist_ok=True)
    
    data_dir = data_dir or Path("data")
    
    try:
        # Process all historical data files
        for file in data_dir.glob("historical_*.json"):
            try:
                # Extract currency pair from filename
                pair = file.stem.replace("historical_", "").replace("_", "/")
                
                with open(file, "r") as f:
                    data = json.load(f)
                
                if validate_historical_data(data):
                    save_historical_to_csv(data, pair, output_dir)
                else:
                    logger.warning(f"Invalid historical data format in {file}")
                    
            except Exception as e:
                logger.error(f"Error processing {file}: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Error processing historical data: {str(e)}")
        raise

if __name__ == '__main__':
    setup_logging()
    process_historical_data()

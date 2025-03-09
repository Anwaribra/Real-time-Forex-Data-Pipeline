import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
import json
import time

from data_ingestion.fetch_data import fetch_forex_data
from data_ingestion.fetch_historical_data import fetch_historical_data
# from data_processing.transform_data import aggregate_yearly_data as transform_data
from data_storage.save_to_csv import process_all_forex_data

def setup_logging() -> None:
    """Configure logging settings."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )

def load_config():
    """Load configuration from config file."""
    with open("config/config.json", "r") as f:
        return json.load(f)

def run_pipeline(fetch_realtime: bool = True, fetch_historical: bool = True, 
                transform: bool = True) -> None:
    """
    Run the complete data pipeline.
    
    Args:
        fetch_realtime: Whether to fetch real-time data
        fetch_historical: Whether to fetch historical data
        transform: Whether to transform and analyze data
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting forex data pipeline")
    
    try:
        # Load configuration
        config = load_config()
        currency_pairs = config["currency_pairs"]
        api_url = config["forex_api_url"]
        api_key = config["forex_api_key"]
        
        # Ensure directories exist
        Path("data").mkdir(exist_ok=True)
        Path("processed_data").mkdir(exist_ok=True)
        
        # Fetch real-time data
        if fetch_realtime:
            logger.info("Fetching real-time forex data")
            fetch_forex_data()
        
        # Fetch historical data
        if fetch_historical:
            logger.info("Fetching historical forex data")
            for pair in currency_pairs:
                from_currency, to_currency = pair.split('/')
                logger.info(f"Fetching historical data for {pair}")
                data = fetch_historical_data(
                    from_currency=from_currency,
                    to_currency=to_currency,
                    api_key=api_key,
                    api_url=api_url
                )
                if data:
                    logger.info(f"Successfully fetched historical data for {pair}")
                time.sleep(15)  # Respect API rate limits
        
        # Transform and analyze data
        if transform:
            logger.info("Processing all forex data")
            process_all_forex_data()
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

def cleanup_old_files(days: Optional[int] = 7) -> None:
    """
    Clean up old data files.
    
    Args:
        days: Number of days to keep files (default: 7)
    """
    logger = logging.getLogger(__name__)
    
    try:
        from datetime import datetime, timedelta
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Clean up data files
        data_dir = Path("data")
        for file in data_dir.glob("*"):
            if file.stat().st_mtime < cutoff_date.timestamp():
                file.unlink()
                logger.info(f"Deleted old file: {file}")
                
        # Clean up processed files
        processed_dir = Path("processed_data")
        for file in processed_dir.glob("*"):
            if file.stat().st_mtime < cutoff_date.timestamp():
                file.unlink()
                logger.info(f"Deleted old processed file: {file}")
                
        # Clean up old logs
        logs_dir = Path("logs")
        for file in logs_dir.glob("pipeline_*.log"):
            if file.stat().st_mtime < cutoff_date.timestamp():
                file.unlink()
                logger.info(f"Deleted old log file: {file}")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")

if __name__ == "__main__":
    # Set up logging
    setup_logging()
    
    try:
        # Run the complete pipeline
        run_pipeline(
            fetch_realtime=True,
            fetch_historical=True,
            transform=True
        )
        
        # Optional: Clean up old files
        cleanup_old_files(days=7)
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise

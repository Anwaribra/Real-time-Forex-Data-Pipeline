import logging
from pathlib import Path
from datetime import datetime, timedelta
import json

from data_ingestion.fetch_data import fetch_forex_data
from data_ingestion.fetch_historical_data import fetch_historical_data
from data_storage.save_to_csv import process_all_forex_data

def setup_logging():
    """Configure logging settings"""
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
    """Load configuration from config file"""
    with open("config/config.json", "r") as f:
        return json.load(f)

def run_pipeline():
    """Run the complete data pipeline"""
    logger = logging.getLogger(__name__)
    logger.info("Starting forex data pipeline")
    
    try:
        # Load configuration
        config = load_config()
        currency_pairs = config["currency_pairs"]
        
        # Fetch real-time data
        for pair in currency_pairs:
            data = fetch_forex_data()
            if data:
                process_all_forex_data(data)
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

def cleanup_old_files(days: int = 7):
    """Clean up old data files"""
    logger = logging.getLogger(__name__)
    cutoff_date = datetime.now() - timedelta(days=days)
    
    try:
        for directory in ['data', 'processed_data', 'logs']:
            dir_path = Path(directory)
            if dir_path.exists():
                for file in dir_path.glob('*'):
                    if file.stat().st_mtime < cutoff_date.timestamp():
                        file.unlink()
                        logger.info(f"Deleted old file: {file}")
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")

if __name__ == "__main__":
    setup_logging()
    
    try:
        run_pipeline()
        cleanup_old_files(days=7)
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise

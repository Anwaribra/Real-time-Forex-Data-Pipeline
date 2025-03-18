from pathlib import Path
import os
import logging

def cleanup_forex_files():
    """
    Remove all forex rate files from the data directory.
    Only keeps the historical_*.json files.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    data_dir = Path(__file__).parent.parent / 'data'
    
    # Files to remove - all forex_rates_* files
    forex_files = list(data_dir.glob('forex_rates_*'))
    
    for file in forex_files:
        try:
            os.remove(file)
            logger.info(f"Removed file: {file}")
        except Exception as e:
            logger.error(f"Error removing {file}: {e}")
            
    logger.info(f"Cleanup complete. Removed {len(forex_files)} files.")

if __name__ == "__main__":
    cleanup_forex_files() 
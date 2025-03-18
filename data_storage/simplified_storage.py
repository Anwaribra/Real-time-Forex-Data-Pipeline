import os
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Custom JSON encoder to handle timestamps
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def save_locally(df, table_name="FOREX_RATES"):
    """Save DataFrame to local SQLite or CSV file as fallback"""
    if df.empty:
        logger.warning("Empty DataFrame provided, nothing to save")
        return False
    
    try:
        # Create data directory
        data_dir = Path(__file__).parent.parent / 'data'
        data_dir.mkdir(exist_ok=True)
        
        # Save as CSV with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name.lower()}_{timestamp}.csv"
        filepath = data_dir / filename
        
        df.to_csv(filepath, index=False)
        logger.info(f"Data saved locally to {filepath}")
        
        # Also save as JSON for easier consumption
        json_file = filepath.with_suffix('.json')
        
        # Convert to dictionary format
        results = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            # Convert timestamps to strings for JSON serialization
            for key, value in row_dict.items():
                if isinstance(value, (pd.Timestamp, datetime)):
                    row_dict[key] = value.isoformat()
            results.append(row_dict)
        
        # Save to JSON
        with open(json_file, 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "data": results
            }, f, indent=2)
        
        logger.info(f"JSON data saved to {json_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving data locally: {str(e)}")
        return False

def try_snowflake_then_local(df, table_name="FOREX_RATES"):
    """Try to save to Snowflake, fall back to local storage if it fails"""
    try:
        # Import the original Snowflake function
        from data_storage.save_to_snowflake import save_to_snowflake
        
        # Try to save to Snowflake
        logger.info("Attempting to save to Snowflake...")
        success = save_to_snowflake(df, table_name)
        
        if success:
            logger.info("Successfully saved to Snowflake")
            return True
        else:
            logger.warning("Failed to save to Snowflake, falling back to local storage")
            return save_locally(df, table_name)
            
    except ImportError:
        logger.warning("Snowflake module not available, using local storage")
        return save_locally(df, table_name)
    except Exception as e:
        logger.error(f"Error in storage attempt: {str(e)}")
        return save_locally(df, table_name)

# For testing
if __name__ == "__main__":
    # Create test data
    test_data = {
        'from_currency': ['USD', 'EUR', 'GBP'],
        'to_currency': ['EUR', 'USD', 'USD'],
        'exchange_rate': [0.92, 1.09, 1.27],
        'last_refreshed': ['2024-03-18 12:00:00'] * 3,
        'timestamp': pd.to_datetime(['2024-03-18 12:00:00'] * 3)
    }
    test_df = pd.DataFrame(test_data)
    
    # Test the function
    success = try_snowflake_then_local(test_df)
    print(f"Storage {'successful' if success else 'failed'}") 
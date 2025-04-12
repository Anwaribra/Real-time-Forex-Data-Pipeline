import os
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('forex_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def normalize_column_names(df):
    """Normalize column names from different formats"""
    column_map = {
        'date': 'date',
        'Date': 'date',
        'open': 'open',
        'Open': 'open',
        '1. open': 'open',
        'high': 'high',
        'High': 'high',
        '2. high': 'high',
        'low': 'low',
        'Low': 'low',
        '3. low': 'low',
        'close': 'close',
        'Close': 'close',
        '4. close': 'close'
    }
    
    df.rename(columns=column_map, inplace=True)
    
    required_columns = ['date', 'open', 'high', 'low', 'close']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return df

def process_forex_data():
    """Process raw forex data from CSV files"""
    try:
        # Load data from CSV files
        data_dir = Path("data")
        for file_path in data_dir.glob("historical_*_*.csv"):
            try:
                # Extract currency pair from filename
                pair = file_path.stem.replace('historical_', '')  
                display_pair = pair.replace('_', '/')  
                
                logger.info(f"Processing {display_pair} from {file_path}")
                
                # Read and normalize data
                df = normalize_column_names(pd.read_csv(file_path))
                df['date'] = pd.to_datetime(df['date'])
                
               
                records = []
                for _, row in df.iterrows():
                    records.append({
                        'date': row['date'].strftime('%Y-%m-%d'),
                        'currency_pair': display_pair,
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close'])
                    })
                
                # Convert to DataFrame for easier handling
                forex_df = pd.DataFrame(records)
                
                # Return the processed data
                yield forex_df
                
                logger.info(f"Processed {len(records)} records for {display_pair}")
            
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    # Process raw data only
    for forex_data in process_forex_data():
        print(f"Processed {len(forex_data)} records")
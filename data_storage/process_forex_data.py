import os
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from snowflake.connector import connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('forex_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_snowflake_connection():
    """Create Snowflake connection"""
    try:
        conn = connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

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

def save_to_snowflake(df, table_name, conn):
    """Save DataFrame to Snowflake table"""
    try:
        # Create cursor
        cursor = conn.cursor()
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            currency_pair VARCHAR(7),
            open_rate FLOAT,
            high_rate FLOAT,
            low_rate FLOAT,
            close_rate FLOAT,
            inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_sql)
        
        # Prepare data for insertion
        values = []
        for _, row in df.iterrows():
            values.append((
                row['date'],
                row['currency_pair'],
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close'])
            ))
        
        # Insert data with upsert
        insert_sql = f"""
        INSERT INTO {table_name} (date, currency_pair, open_rate, high_rate, low_rate, close_rate)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, currency_pair) DO UPDATE SET
            open_rate = EXCLUDED.open_rate,
            high_rate = EXCLUDED.high_rate,
            low_rate = EXCLUDED.low_rate,
            close_rate = EXCLUDED.close_rate,
            inserted_at = CURRENT_TIMESTAMP()
        """
        
        cursor.executemany(insert_sql, values)
        conn.commit()
        
        logger.info(f"Successfully saved {len(values)} records to {table_name}")
        
    except Exception as e:
        logger.error(f"Error saving to Snowflake: {e}")
        raise
    finally:
        cursor.close()

def process_forex_data():
    """Process raw forex data from CSV files and save to Snowflake"""
    try:
        # Get Snowflake connection
        conn = get_snowflake_connection()
        
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
                
                # Process records
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
                
                # Convert to DataFrame
                forex_df = pd.DataFrame(records)
                
                # Save to Snowflake
                table_name = f"FOREX_RATES_{pair.replace('/', '_')}"
                save_to_snowflake(forex_df, table_name, conn)
                
                logger.info(f"Processed {len(records)} records for {display_pair}")
            
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
                continue
        
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    process_forex_data()
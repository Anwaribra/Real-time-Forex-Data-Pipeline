import os
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
from snowflake.connector import connect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('snowflake_update.log'),
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
        
        # Set the database context
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}")
        cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}")
        cursor.close()
        
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

def get_latest_date(conn, table_name):
    """Get the latest date from the specified table"""
    try:
        cursor = conn.cursor()
        query = f"""
        SELECT MAX(date) as latest_date 
        FROM {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.{table_name}
        """
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result and result[0] else None
    except Exception as e:
        logger.error(f"Error getting latest date: {e}")
        raise
    finally:
        cursor.close()

def update_snowflake_data(conn, table_name, new_data):
    """Update Snowflake table with new data"""
    try:
        cursor = conn.cursor()
        
        # Prepare data for insertion
        values = []
        for _, row in new_data.iterrows():
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
        INSERT INTO {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}.{table_name} 
        (date, currency_pair, open_rate, high_rate, low_rate, close_rate)
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
        
        logger.info(f"Successfully updated {len(values)} records in {table_name}")
        
    except Exception as e:
        logger.error(f"Error updating Snowflake data: {e}")
        raise
    finally:
        cursor.close()

def fetch_new_data(currency_pair, start_date):
    """Fetch new forex data from your data source"""
    # Replace this with your actual data fetching logic
    # This is just a placeholder
    try:
        # Example: Read from CSV file
        file_path = f"data/historical_{currency_pair.replace('/', '_')}.csv"
        df = pd.read_csv(file_path)
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'] > start_date]
        return df
    except Exception as e:
        logger.error(f"Error fetching new data: {e}")
        return None

def update_forex_data():
    """Main function to update forex data in Snowflake"""
    try:
        # Get Snowflake connection
        conn = get_snowflake_connection()
        
        # Define currency pairs to update
        currency_pairs = ['EUR/USD', 'EUR/EGP', 'USD/EGP']
        
        for pair in currency_pairs:
            try:
                # Get table name
                table_name = f"FOREX_RATES_{pair.replace('/', '_')}"
                
                # Get latest date from Snowflake
                latest_date = get_latest_date(conn, table_name)
                if latest_date:
                    start_date = latest_date
                else:
                    # If no data exists, get last 30 days
                    start_date = datetime.now() - timedelta(days=30)
                
                logger.info(f"Updating {pair} from {start_date}")
                
                # Fetch new data
                new_data = fetch_new_data(pair, start_date)
                if new_data is not None and not new_data.empty:
                    # Update Snowflake
                    update_snowflake_data(conn, table_name, new_data)
                else:
                    logger.info(f"No new data available for {pair}")
            
            except Exception as e:
                logger.error(f"Error processing {pair}: {e}")
                continue
        
    except Exception as e:
        logger.error(f"Error in update process: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    update_forex_data()
import os
import pandas as pd
import snowflake.connector
import logging
import json
from dotenv import load_dotenv
import tempfile
import csv

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_snowflake_connection():
    """Get Snowflake connection parameters from environment variables"""
    return {
        'user': os.environ.get('SNOWFLAKE_USER'),
        'password': os.environ.get('SNOWFLAKE_PASSWORD'),
        'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'database': os.environ.get('SNOWFLAKE_DATABASE'),
        'schema': os.environ.get('SNOWFLAKE_SCHEMA')
    }

def save_to_snowflake(df):
    """Save DataFrame to Snowflake"""
    conn = None
    try:
        # Get Snowflake connection parameters
        snow_conn = get_snowflake_connection()
        
        logger.info(f"Connecting to Snowflake with account: {snow_conn['account']}")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=snow_conn['user'],
            password=snow_conn['password'],
            account=snow_conn['account'],
            warehouse=snow_conn['warehouse']
        )
        
        # Create database and schema if they don't exist
        cursor = conn.cursor()
        
        # Create database if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {snow_conn['database']}")
        logger.info(f"Created or verified database: {snow_conn['database']}")
        
        # Use the database
        cursor.execute(f"USE DATABASE {snow_conn['database']}")
        
        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snow_conn['schema']}")
        logger.info(f"Created or verified schema: {snow_conn['schema']}")
        
        # Use the schema
        cursor.execute(f"USE SCHEMA {snow_conn['schema']}")
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS FOREX_RATES (
            from_currency VARCHAR(10),
            to_currency VARCHAR(10),
            exchange_rate FLOAT,
            last_refreshed VARCHAR(30),
            timestamp TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        cursor.execute(create_table_sql)
        logger.info("Created or verified table: FOREX_RATES")
        
        # Save DataFrame to a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            df.to_csv(temp_file.name, index=False, quoting=csv.QUOTE_NONNUMERIC)
            temp_file_path = temp_file.name
        
        # Create a stage if it doesn't exist
        cursor.execute("CREATE STAGE IF NOT EXISTS forex_stage")
        
        # Upload the file to the stage
        cursor.execute(f"PUT file://{temp_file_path} @forex_stage")
        
        # Copy data from stage to table
        copy_sql = """
        COPY INTO FOREX_RATES (from_currency, to_currency, exchange_rate, last_refreshed, timestamp)
        FROM @forex_stage
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        """
        cursor.execute(copy_sql)
        
        # Remove the temporary file
        os.unlink(temp_file_path)
        
        # Get the number of rows inserted
        cursor.execute("SELECT COUNT(*) FROM FOREX_RATES")
        row_count = cursor.fetchone()[0]
        
        logger.info(f"Successfully loaded data into Snowflake. Total rows in table: {row_count}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return True
            
    except Exception as e:
        logger.error(f"Error saving to Snowflake: {str(e)}")
        if conn:
            conn.close()
        raise

# For testing the module directly
if __name__ == "__main__":
    # Create a test DataFrame
    test_data = {
        'from_currency': ['USD', 'EUR'],
        'to_currency': ['EUR', 'USD'],
        'exchange_rate': [0.85, 1.18],
        'last_refreshed': ['2023-03-17 12:00:00', '2023-03-17 12:00:00'],
        'timestamp': pd.to_datetime(['2023-03-17 12:00:00', '2023-03-17 12:00:00'])
    }
    test_df = pd.DataFrame(test_data)
    
    # Test the function
    print("Testing Snowflake connection...")
    save_to_snowflake(test_df)
    print("Test completed successfully!") 
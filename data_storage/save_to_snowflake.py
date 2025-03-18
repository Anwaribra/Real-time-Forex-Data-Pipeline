import os
import pandas as pd
import snowflake.connector
import logging
import tempfile
import csv
from pathlib import Path
from dotenv import load_dotenv

# Setup logging with more details
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def get_snowflake_connection():
    """Get Snowflake connection parameters from environment variables"""
    required_vars = ['SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT', 
                    'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA']
    
    # Check if all required variables are set
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return {
        'user': os.environ.get('SNOWFLAKE_USER'),
        'password': os.environ.get('SNOWFLAKE_PASSWORD'),
        'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'database': os.environ.get('SNOWFLAKE_DATABASE'),
        'schema': os.environ.get('SNOWFLAKE_SCHEMA')
    }

def save_to_snowflake(df, table_name='FOREX_RATES'):
    """Save DataFrame to Snowflake with enhanced debugging"""
    if df.empty:
        logger.warning("Empty DataFrame provided, nothing to save")
        return False
        
    conn = None
    cursor = None
    temp_file_path = None
    
    try:
        # Get Snowflake connection parameters
        snow_conn = get_snowflake_connection()
        
        logger.info(f"Connecting to Snowflake: {snow_conn['account']}")
        
        # Connect with auto-commit disabled
        conn = snowflake.connector.connect(
            user=snow_conn['user'],
            password=snow_conn['password'],
            account=snow_conn['account'],
            warehouse=snow_conn['warehouse'],
            autocommit=False,
            login_timeout=30
        )
        
        cursor = conn.cursor()
        
        # Check current role and permissions
        cursor.execute("SELECT CURRENT_ROLE()")
        current_role = cursor.fetchone()[0]
        logger.info(f"Current Snowflake role: {current_role}")
        
        # Resume warehouse if suspended
        cursor.execute(f"ALTER WAREHOUSE {snow_conn['warehouse']} RESUME IF SUSPENDED")
        logger.info(f"Warehouse {snow_conn['warehouse']} resumed")
        
        # Create database and schema
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {snow_conn['database']}")
        cursor.execute(f"USE DATABASE {snow_conn['database']}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snow_conn['schema']}")
        cursor.execute(f"USE SCHEMA {snow_conn['schema']}")
        
        # Create table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            from_currency VARCHAR(10),
            to_currency VARCHAR(10),
            exchange_rate FLOAT,
            last_refreshed VARCHAR(30),
            timestamp TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_sql)
        
        # Save to CSV with debugging
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            df.to_csv(temp_file.name, index=False, quoting=csv.QUOTE_NONNUMERIC)
            temp_file_path = temp_file.name
        
        # Log file preview
        logger.info(f"CSV file contents preview:")
        with open(temp_file_path, 'r') as f:
            preview = f.readlines()[:5]
            for line in preview:
                logger.info(line.strip())
        
        # Create stage
        stage_name = f"{table_name.lower()}_stage"
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
        
        # Upload to stage
        put_command = f"PUT file://{temp_file_path} @{stage_name} OVERWRITE=TRUE"
        cursor.execute(put_command)
        put_result = cursor.fetchall()
        logger.info(f"PUT command result: {put_result}")
        
        # List files in stage
        cursor.execute(f"LIST @{stage_name}")
        stage_files = cursor.fetchall()
        logger.info(f"Files in stage: {stage_files}")
        
        # Execute COPY without validation mode
        copy_sql = f"""
        COPY INTO {table_name} (from_currency, to_currency, exchange_rate, last_refreshed, timestamp)
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE'
        """
        
        cursor.execute(copy_sql)
        copy_result = cursor.fetchall()
        logger.info(f"COPY command result: {copy_result}")
        
        # Verify data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        logger.info(f"Row count after load: {row_count}")
        
        # Commit transaction
        conn.commit()
        logger.info("Transaction committed successfully")
        
        return True
            
    except Exception as e:
        logger.error(f"Error saving to Snowflake: {str(e)}")
        if conn:
            try:
                conn.rollback()
                logger.info("Transaction rolled back")
            except:
                pass
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)

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
    success = save_to_snowflake(test_df)
    
    if success:
        print("✅ Test completed successfully!")
    else:
        print("❌ Test failed!") 
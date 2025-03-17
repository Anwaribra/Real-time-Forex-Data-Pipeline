import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import json
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import logging
from airflow.hooks.base import BaseHook

def get_snowflake_connection():
    """Get Snowflake connection parameters from Airflow connection"""
    conn = BaseHook.get_connection('snowflake_default')
    return {
        'user': conn.login,
        'password': conn.password,
        'account': conn.extra_dejson.get('account'),
        'warehouse': conn.extra_dejson.get('warehouse'),
        'database': conn.extra_dejson.get('database'),
        'schema': conn.extra_dejson.get('schema')
    }

def save_to_snowflake(df):
    """Save DataFrame to Snowflake"""
    logger = logging.getLogger(__name__)
    
    try:
        # Get Snowflake connection parameters
        snow_conn = get_snowflake_connection()
        
        # Connect to Snowflake
        with connect(
            user=snow_conn['user'],
            password=snow_conn['password'],
            account=snow_conn['account'],
            warehouse=snow_conn['warehouse'],
            database=snow_conn['database'],
            schema=snow_conn['schema']
        ) as conn:
            
            # Create table if not exists
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS FOREX_RATES (
                from_currency VARCHAR(10),
                to_currency VARCHAR(10),
                exchange_rate FLOAT,
                last_refreshed TIMESTAMP,
                timestamp TIMESTAMP,
                date DATE,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            
            # Write DataFrame to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name='FOREX_RATES',
                database=snow_conn['database'],
                schema=snow_conn['schema']
            )
            
            logger.info(f"Successfully loaded {nrows} rows into Snowflake")
            return True
            
    except Exception as e:
        logger.error(f"Error saving to Snowflake: {str(e)}")
        raise

def save_to_snowflake():
    load_dotenv()
    
    
    with open('config/config.json', 'r') as f:
        config = json.load(f)
    
    
    engine = create_engine(URL(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    ))
    
    
    current_rates = pd.read_csv('temp_data/current_rates.csv')
    historical_rates = pd.read_csv('temp_data/historical_rates.csv')
    
    
    all_rates = pd.concat([current_rates, historical_rates])
    

    table_name = config['snowflake']['table_name']
    all_rates.to_sql(
        table_name,
        engine,
        if_exists='append',
        index=False
    )
    
    
    os.remove('temp_data/current_rates.csv')
    os.remove('temp_data/historical_rates.csv')

if __name__ == "__main__":
    save_to_snowflake() 
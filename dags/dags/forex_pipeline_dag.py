from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
import logging
import time
from pathlib import Path
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from data_storage.save_to_snowflake import save_to_snowflake

# Define paths
DAG_PATH = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(DAG_PATH)), 'config', 'config.json')
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(DAG_PATH)), 'data')

# Create required directories
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(DATA_PATH, 'forex_pipeline.log')),
        logging.StreamHandler()
    ]
)

# Load configuration
with open(CONFIG_PATH, 'r') as config_file:
    config = json.load(config_file)

default_args = {
    'owner': 'anwar',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 8),
    'email': ['anwarmousa100@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def fetch_realtime_rates(**context):
    """Fetch current forex rates from API"""
    logger = logging.getLogger(__name__)
    results = []
    
    try:
        for pair in config['currency_pairs']:
            from_currency, to_currency = pair.split('/')
            params = {
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": from_currency,
                "to_currency": to_currency,
                "apikey": config['forex_api_key']
            }
            
            try:
                response = requests.get(config['forex_api_url'], params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if 'Realtime Currency Exchange Rate' in data:
                    rate_data = data['Realtime Currency Exchange Rate']
                    results.append({
                        'from_currency': from_currency,
                        'to_currency': to_currency,
                        'exchange_rate': float(rate_data['5. Exchange Rate']),
                        'last_refreshed': rate_data['6. Last Refreshed'],
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.info(f"Successfully fetched rate for {pair}")
                
                time.sleep(1)  
                
            except Exception as e:
                logger.error(f"Error fetching {pair}: {str(e)}")
                
        if results:
            output_file = os.path.join(DATA_PATH, f'realtime_rates_{datetime.now().strftime("%Y%m%d_%H%M")}.csv')
            pd.DataFrame(results).to_csv(output_file, index=False)
            return output_file
            
    except Exception as e:
        logger.error(f"Fatal error in fetch_realtime_rates: {str(e)}")
        raise
    
    return None

def fetch_historical_rates(**context):
    """Fetch historical forex data"""
    logger = logging.getLogger(__name__)
    
    try:
        for pair in config['currency_pairs']:
            from_currency, to_currency = pair.split('/')
            params = {
                "function": "FX_DAILY",
                "from_symbol": from_currency,
                "to_symbol": to_currency,
                "apikey": config['forex_api_key'],
                "outputsize": "compact"  # Last 100 days
            }
            
            try:
                response = requests.get(config['forex_api_url'], params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if "Time Series FX (Daily)" in data:
                    output_file = os.path.join(
                        DATA_PATH, 
                        f'historical_{from_currency}_{to_currency}_{datetime.now().strftime("%Y%m%d")}.json'
                    )
                    with open(output_file, 'w') as f:
                        json.dump(data, f, indent=4)
                    logger.info(f"Saved historical data for {pair}")
                
                time.sleep(15)  # Respect API rate limits
                
            except Exception as e:
                logger.error(f"Error fetching historical data for {pair}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Fatal error in fetch_historical_rates: {str(e)}")
        raise

def process_forex_data(**context):
    """Process the forex data and save to Snowflake"""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    # Get file paths from XCom
    realtime_file = ti.xcom_pull(task_ids='fetch_realtime_rates')
    historical_file = ti.xcom_pull(task_ids='fetch_historical_rates')
    
    try:
        # Process data
        df = pd.read_csv(realtime_file)
        # Add any additional processing here
        
        # Save to Snowflake
        save_to_snowflake(df)
        
        return realtime_file
        
    except Exception as e:
        logger.error(f"Error processing forex data: {str(e)}")
        raise

with DAG(
    'forex_data_pipeline',
    default_args=default_args,
    description='DAG to fetch and process forex exchange rates',
    schedule_interval='daily',  
    tags=['forex', 'currency']
) as dag:

    fetch_realtime = PythonOperator(
        task_id='fetch_realtime_rates',
        python_callable=fetch_realtime_rates,
        provide_context=True,
    )

    fetch_historical = PythonOperator(
        task_id='fetch_historical_rates',
        python_callable=fetch_historical_rates,
        provide_context=True,
    )

    process_data = PythonOperator(
        task_id='process_forex_data',
        python_callable=process_forex_data,
        provide_context=True,
    )

    # Define task dependencies
    [fetch_realtime, fetch_historical] >> process_data
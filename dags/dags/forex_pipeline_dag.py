import os
import json
import requests
import pandas as pd
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta

# Only import Airflow when not running the file directly
if __name__ != "__main__":
    from airflow import DAG
    from airflow.operators.python import PythonOperator

# Define paths
DAG_PATH = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAG_PATH))
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'config.json')
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')

# Create required directories
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration
with open(CONFIG_PATH, 'r') as config_file:
    config = json.load(config_file)

def fetch_forex_rates(**context):
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
            
            # Respect API rate limits
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"Error fetching forex rates: {str(e)}")
        raise
        
    if results:
        output_file = os.path.join(DATA_PATH, f'forex_rates_{datetime.now().strftime("%Y%m%d_%H%M")}.csv')
        pd.DataFrame(results).to_csv(output_file, index=False)
        return output_file
    
    return None

def process_forex_data(**context):
    """Process forex data and save to database"""
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    forex_file = ti.xcom_pull(task_ids='fetch_forex_rates')
    
    if not forex_file:
        raise ValueError("No forex data file found")
        
    try:
        # Read and process the data
        df = pd.read_csv(forex_file)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Import here to avoid Airflow initialization when running directly
        import sys
        sys.path.append(PROJECT_ROOT)
        from data_storage.save_to_snowflake import save_to_snowflake
        save_to_snowflake(df)
        
        logger.info("Successfully processed and saved forex data")
        return forex_file
        
    except Exception as e:
        logger.error(f"Error processing forex data: {str(e)}")
        raise

# Only create the DAG when not running the file directly
if __name__ != "__main__":
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

    with DAG(
        'forex_data_pipeline',
        default_args=default_args,
        description='Fetch and process forex exchange rates',
        schedule_interval='@daily',
        catchup=False,
        tags=['forex']
    ) as dag:
        fetch_rates = PythonOperator(
            task_id='fetch_forex_rates',
            python_callable=fetch_forex_rates
        )

        process_data = PythonOperator(
            task_id='process_forex_data',
            python_callable=process_forex_data
        )

        # Define task dependencies
        fetch_rates >> process_data

# For testing when running directly
if __name__ == "__main__":
    print("Testing forex data pipeline...")
    
    # Test fetch_forex_rates
    context = {'execution_date': datetime.now()}
    forex_file = fetch_forex_rates(**context)
    
    if forex_file:
        print(f"Successfully fetched forex rates: {forex_file}")
        
        # Test process_forex_data (mock task instance)
        class MockTaskInstance:
            def xcom_pull(self, task_ids):
                return forex_file
                
        context['task_instance'] = MockTaskInstance()
        process_forex_data(**context)
    else:
        print("Failed to fetch forex rates")
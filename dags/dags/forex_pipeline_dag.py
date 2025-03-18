import os
import json
import requests
import pandas as pd
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
from pendulum import timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# Define paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'config.json')
DATA_PATH = os.path.join(PROJECT_ROOT, 'data')

# Create data directory if needed
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

# Load configuration once
with open(CONFIG_PATH, 'r') as config_file:
    config = json.load(config_file)

def fetch_forex_rates(**context):
    """Fetch current forex rates from API"""
    logger = logging.getLogger("airflow.task")
    results = []
    
    for pair in config['currency_pairs']:
        try:
            from_currency, to_currency = pair.split('/')
            
            # API request parameters
            params = {
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": from_currency,
                "to_currency": to_currency,
                "apikey": config['forex_api_key']
            }
            
            # Make API request
            response = requests.get(config['forex_api_url'], params=params, timeout=10)
            data = response.json()
            
            # Extract exchange rate
            if 'Realtime Currency Exchange Rate' in data:
                rate_data = data['Realtime Currency Exchange Rate']
                results.append({
                    'from_currency': from_currency,
                    'to_currency': to_currency,
                    'exchange_rate': float(rate_data['5. Exchange Rate']),
                    'timestamp': datetime.now().isoformat()
                })
                logger.info(f"Fetched {from_currency}/{to_currency} rate")
            else:
                logger.warning(f"No rate data for {pair}: {data.keys()}")
            
            # API rate limiting
            context['ti'].xcom_push(key=f'last_pair', value=pair)
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error fetching {pair}: {str(e)}")
    
    # Save results to CSV
    if results:
        filename = f'forex_rates_{datetime.now().strftime("%Y%m%d")}.csv'
        output_file = os.path.join(DATA_PATH, filename)
        pd.DataFrame(results).to_csv(output_file, index=False)
        logger.info(f"Saved {len(results)} rates to {filename}")
        return output_file
    
    return None

def process_forex_data(**context):
    """Create JSON with structured forex rates data"""
    logger = logging.getLogger("airflow.task")
    
    # Get input file from previous task
    ti = context['ti']
    forex_file = ti.xcom_pull(task_ids='fetch_forex_rates')
    
    if not forex_file or not os.path.exists(forex_file):
        logger.error("No forex data file found")
        return False
    
    try:
        # Read CSV
        df = pd.read_csv(forex_file)
        
        # Create results dictionary
        results = {
            "timestamp": datetime.now().isoformat(),
            "rates": {}
        }
        
        # Process each currency pair
        for _, row in df.iterrows():
            key = f"{row['from_currency']}_{row['to_currency']}"
            results["rates"][key] = row['exchange_rate']
        
        # Save to JSON
        json_file = forex_file.replace('.csv', '.json')
        with open(json_file, 'w') as f:
            json.dump(results, f, indent=2)
            
        logger.info(f"Processed data saved to {json_file}")
        return json_file
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return False


# DAG definition
default_args = {
    'owner': 'anwar',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 8, tzinfo=timezone('UTC')),
    'email': ['anwarmousa100@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'forex_pipeline',
    default_args=default_args,
    description='Forex exchange rates pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['forex'],
) as dag:
    # Task 1: Fetch forex rates
    fetch_task = PythonOperator(
        task_id='fetch_forex_rates',
        python_callable=fetch_forex_rates,
    )
    
    # Task 2: Process and transform data
    process_task = PythonOperator(
        task_id='process_forex_data',
        python_callable=process_forex_data,
    )
    
    # Set task dependencies
    fetch_task >> process_task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path
import logging

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import your modules
from data_ingestion.fetch_data import fetch_forex_data
from data_ingestion.fetch_historical_data import fetch_historical_data
from data_storage.save_to_csv import process_all_forex_data

# Configure logging
logger = logging.getLogger(__name__)

def fetch_data_wrapper(**context):
    """Wrapper function for fetch_forex_data"""
    try:
        logger.info("Starting real-time forex data fetch")
        result = fetch_forex_data()
        context['task_instance'].xcom_push(key='forex_data', value=result)
        logger.info("Successfully fetched real-time forex data")
        return result
    except Exception as e:
        logger.error(f"Error fetching real-time forex data: {str(e)}")
        raise

def fetch_historical_wrapper(**context):
    """Wrapper function for fetch_historical_data"""
    try:
        logger.info("Starting historical forex data fetch")
        result = fetch_historical_data()
        context['task_instance'].xcom_push(key='historical_data', value=result)
        logger.info("Successfully fetched historical forex data")
        return result
    except Exception as e:
        logger.error(f"Error fetching historical forex data: {str(e)}")
        raise

def save_data_wrapper(**context):
    """Wrapper function for process_all_forex_data"""
    try:
        logger.info("Starting data save process")
        # Get data from previous tasks using XCom
        forex_data = context['task_instance'].xcom_pull(task_ids='fetch_forex_data', key='forex_data')
        historical_data = context['task_instance'].xcom_pull(task_ids='fetch_historical_data', key='historical_data')
        
        # Process and save data
        result = process_all_forex_data(forex_data, historical_data)
        logger.info("Successfully saved forex data")
        return result
    except Exception as e:
        logger.error(f"Error saving forex data: {str(e)}")
        raise

# Define DAG
default_args = {
    'owner': 'anwar',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(minutes=10),
}

# Create DAG
with DAG(
    dag_id='forex_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch, transform, and store Forex data daily',
    schedule='@daily',
    catchup=False,
    tags=['forex', 'data_pipeline'],
    doc_md="""
    # Forex Data Pipeline DAG
    
    This DAG performs the following operations:
    1. Fetches real-time forex data
    2. Fetches historical forex data
    3. Processes and saves the data
    
    ## Dependencies
    - Alpha Vantage API
    - Kafka (for streaming)
    - PostgreSQL (for storage)
    
    ## Configuration
    The DAG uses environment variables for configuration:
    - ALPHA_VANTAGE_API_KEY: API key for Alpha Vantage
    """,
) as dag:
    
    # Task to fetch real-time forex data
    fetch_data_task = PythonOperator(
        task_id='fetch_forex_data',
        python_callable=fetch_data_wrapper,
        provide_context=True,
        doc_md="""
        ### Fetch Real-time Forex Data
        Fetches current exchange rates from Alpha Vantage API
        """,
    )
    
    # Task to fetch historical forex data
    fetch_historical_task = PythonOperator(
        task_id='fetch_historical_data',
        python_callable=fetch_historical_wrapper,
        provide_context=True,
        doc_md="""
        ### Fetch Historical Forex Data
        Fetches historical exchange rates data
        """,
    )
    
    # Task to save data
    save_data_task = PythonOperator(
        task_id='save_forex_data',
        python_callable=save_data_wrapper,
        provide_context=True,
        doc_md="""
        ### Save Forex Data
        Processes and saves both real-time and historical data
        """,
    )
    
    # Set task dependencies
    fetch_data_task >> fetch_historical_task >> save_data_task

# For local testing
if __name__ == "__main__":
    dag.test()
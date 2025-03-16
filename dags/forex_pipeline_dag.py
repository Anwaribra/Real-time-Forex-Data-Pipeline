import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from data_ingestion.fetch_historical_data import fetch_forex_data
from data_storage.save_to_csv import save_data_to_csv

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
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'forex_pipeline',
    default_args=default_args,
    description='Fetch and store currency data',
    schedule_interval=timedelta(days=1),
)

# Data fetch task
fetch_task = PythonOperator(
    task_id='fetch_forex_data',
    python_callable=fetch_forex_data,
    dag=dag,
)

# Data save task
save_task = PythonOperator(
    task_id='save_forex_data',
    python_callable=save_data_to_csv,
    dag=dag,
)

fetch_task >> save_task

# For local testing
if __name__ == "__main__":
    dag.test()
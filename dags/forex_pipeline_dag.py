from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add the project root to Python path
sys.path.append('/opt/airflow')

# Import your modules
from data_ingestion.fetch_data import fetch_forex_data as fetch_data_func
from data_ingestion.fetch_historical_data import fetch_historical_data as fetch_historical_func
from data_transformation.transform_data import transform_data as transform_func
from data_storage.save_to_csv import process_all_forex_data as save_func

# Define default arguments for the DAG
default_args = {
    'owner': 'anwar',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='forex_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch, transform, and store Forex data daily',
    schedule='@daily',
    catchup=False,
) as dag:
    
    # Task to fetch real-time forex data
    fetch_data_task = PythonOperator(
        task_id='fetch_forex_data',
        python_callable=fetch_data_func,
    )
    
    # Task to fetch historical forex data
    fetch_historical_task = PythonOperator(
        task_id='fetch_historical_data',
        python_callable=fetch_historical_func,
    )
    
    # Task to transform data
    transform_data_task = PythonOperator(
        task_id='transform_forex_data',
        python_callable=transform_func,
    )
    
    # Task to save data
    save_data_task = PythonOperator(
        task_id='save_forex_data',
        python_callable=save_func,
    )
    
    # Define task dependencies
    fetch_data_task >> fetch_historical_task >> transform_data_task >> save_data_task

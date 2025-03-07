from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'anwar',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
with DAG(
    dag_id='forex_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch, transform, and store Forex data daily',
    schedule='@daily',
    catchup=False,
) as dag:
    
    # Task to fetch real-time forex data
    def fetch_forex_data():
        script_path = os.path.join(os.getcwd(), 'data_ingestion/fetch_data.py')
        subprocess.run(['python', script_path], check=True)
    
    fetch_data_task = PythonOperator(
        task_id='fetch_forex_data',
        python_callable=fetch_forex_data,
    )
    
    # Task to fetch historical forex data
    def fetch_historical_data():
        script_path = os.path.join(os.getcwd(), 'data_ingestion/fetch_historical_data.py')
        subprocess.run(['python', script_path], check=True)
    
    fetch_historical_task = PythonOperator(
        task_id='fetch_historical_data',
        python_callable=fetch_historical_data,
    )
    
    # Task to transform data
    def transform_data():
        script_path = os.path.join(os.getcwd(), 'data_transformation/transform_data.py')
        subprocess.run(['python', script_path], check=True)
    
    transform_data_task = PythonOperator(
        task_id='transform_forex_data',
        python_callable=transform_data,
    )
    
    # Task to save data to CSV
    def save_data():
        script_path = os.path.join(os.getcwd(), 'data_storage/save_to_csv.py')
        subprocess.run(['python', script_path], check=True)
    
    save_data_task = PythonOperator(
        task_id='save_forex_data',
        python_callable=save_data,
    )
    
    # Define Task Dependencies
    fetch_data_task >> fetch_historical_task >> transform_data_task >> save_data_task

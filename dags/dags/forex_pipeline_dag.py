import os
import json
import pandas as pd
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from pendulum import timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Define paths
PROJECT_ROOT = '/home/anwar/Real-time-Data-Pipeline'
sys.path.append(PROJECT_ROOT)  # Add project root to Python path

# Create data directory if needed
Path(os.path.join(PROJECT_ROOT, 'data')).mkdir(parents=True, exist_ok=True)

def fetch_forex_rates(**context):
    """Fetch current forex rates from API using our fetch_data module"""
    logger = logging.getLogger("airflow.task")
    logger.info("Starting Airflow fetch task")
    
    try:
        # Import our existing fetch functionality
        # First, add the project root to the Python path
        sys.path.insert(0, PROJECT_ROOT)
        from data_ingestion.fetch_data import fetch_forex_data
        
        # Check if we should use mock data (can be set in Airflow UI)
        use_mock = False
        try:
            use_mock_var = Variable.get("use_mock_forex_data", default_var="false")
            use_mock = use_mock_var.lower() == 'true'
        except:
            pass
            
        logger.info(f"Use mock data setting: {use_mock}")
        
        # Fetch data using our existing module
        df = fetch_forex_data(use_mock=use_mock)
        
        if df.empty:
            logger.error("No data fetched")
            return None
            
        logger.info(f"Successfully fetched {len(df)} exchange rates")
        
        # Pass the DataFrame to the next task via XCom
        # First save to a file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_file = os.path.join(PROJECT_ROOT, 'data', f'airflow_forex_{timestamp}.csv')
        df.to_csv(output_file, index=False)
        
        # Also include the result count
        context['ti'].xcom_push(key='record_count', value=len(df))
        return output_file
        
    except Exception as e:
        logger.error(f"Error in fetch_forex_rates: {str(e)}")
        # In case of error, return a flag to use mock data in the next attempt
        context['ti'].xcom_push(key='use_mock_next', value=True)
        return None

def process_and_store_data(**context):
    """Process forex data and store it in Snowflake or locally"""
    logger = logging.getLogger("airflow.task")
    logger.info("Starting data processing and storage task")
    
    try:
        # Add the project root to the Python path
        sys.path.insert(0, PROJECT_ROOT)
        
        # Get input file from previous task
        ti = context['ti']
        forex_file = ti.xcom_pull(task_ids='fetch_forex_rates')
        
        if not forex_file or not os.path.exists(forex_file):
            logger.warning("No forex data file found, generating mock data")
            # If no file exists, use mock data
            from data_ingestion.fetch_data import generate_mock_data
            df = generate_mock_data()
        else:
            # Read the CSV file
            logger.info(f"Reading data from {forex_file}")
            df = pd.read_csv(forex_file)
        
        # Process the data (convert timestamp to datetime)
        logger.info("Processing data...")
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Try storing to Snowflake first, fallback to local
        try:
            from data_storage.save_to_snowflake import save_to_snowflake
            logger.info("Storing data to Snowflake...")
            success = save_to_snowflake(df)
            if success:
                logger.info("Successfully stored data in Snowflake")
            else:
                logger.warning("Failed to store in Snowflake, falling back to local storage")
                from data_storage.save_to_snowflake import save_locally
                success = save_locally(df)
        except ImportError:
            logger.warning("Snowflake module not available, using local storage")
            from data_storage.save_to_snowflake import save_locally
            success = save_locally(df)
        except Exception as e:
            logger.error(f"Error with Snowflake: {str(e)}")
            from data_storage.save_to_snowflake import save_locally
            success = save_locally(df)
            
        return success
            
    except Exception as e:
        logger.error(f"Error in process_and_store_data: {str(e)}")
        return False

# DAG definition
default_args = {
    'owner': 'anwar',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 8, tzinfo=timezone('UTC')),
    'email': ['anwarmousa100@gmail.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'forex_pipeline',
    default_args=default_args,
    description='Forex exchange rates pipeline',
    schedule='@daily',
    catchup=False,
    tags=['forex'],
) as dag:
    # Task 1: Fetch forex rates
    fetch_task = PythonOperator(
        task_id='fetch_forex_rates',
        python_callable=fetch_forex_rates,
    )
    
    # Task 2: Process and store data
    process_store_task = PythonOperator(
        task_id='process_and_store_data',
        python_callable=process_and_store_data,
    )
    
    # Set task dependencies
    fetch_task >> process_store_task
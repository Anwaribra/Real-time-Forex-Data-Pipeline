from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Create the DAG
dag = DAG(
    'forex_pipeline',
    default_args=default_args,
    description='A pipeline to process forex data with multi-layer architecture',
    schedule='@daily',  
    catchup=False
)
# Function to fetch forex rates
def fetch_forex_rates(**context):
    API_KEY = os.getenv('FOREX_API_KEY')
    base_currency = 'USD'
    target_currencies = ['EGP', 'EUR', 'USD']
    
    rates_data = []
    timestamp = datetime.now()
    
    try:
        # Replace with your actual API endpoint
        url = f"https://api.exchangerate-api.com/v4/latest/{base_currency}"
        response = requests.get(url)
        data = response.json()
        
        for currency in target_currencies:
            if currency in data['rates']:
                rate = data['rates'][currency]
                rates_data.append({
                    'timestamp': timestamp,
                    'base_currency': base_currency,
                    'target_currency': currency,
                    'rate': rate
                })
        
        # Save to context for next tasks
        context['task_instance'].xcom_push(key='forex_rates', value=rates_data)
        return "Forex rates fetched successfully"
    
    except Exception as e:
        raise Exception(f"Error fetching forex rates: {str(e)}")

# Function to process and save data
def process_and_save_data(**context):
    rates_data = context['task_instance'].xcom_pull(key='forex_rates', task_ids='fetch_forex_rates')
    df = pd.DataFrame(rates_data)
    
    # Calculate additional metrics
    df['date'] = df['timestamp'].dt.date
    df['time'] = df['timestamp'].dt.time
    
    # Save processed data for Snowflake
    context['task_instance'].xcom_push(key='processed_data', value=df.to_dict('records'))
    return "Data processed successfully"

# Function to update Snowflake bronze layer
def bronze_layer_processing(**context):
    processed_data = context['task_instance'].xcom_pull(key='processed_data', task_ids='process_and_save_data')
    
    # Create bronze layer table if not exists
    create_table_query = """
    CREATE TABLE IF NOT EXISTS FOREX_DATA.BRONZE.RAW_RATES (
        timestamp TIMESTAMP,
        base_currency STRING,
        target_currency STRING,
        rate FLOAT,
        date DATE,
        time TIME
    )
    """
    
    # Insert data query
    insert_query = """
    INSERT INTO FOREX_DATA.BRONZE.RAW_RATES
    VALUES (%(timestamp)s, %(base_currency)s, %(target_currency)s, %(rate)s, %(date)s, %(time)s)
    """
    
    return "Bronze layer updated successfully"

# Function to process silver layer
def silver_layer_processing(**context):
    # Transform bronze to silver layer with data quality checks
    silver_query = """
    INSERT INTO FOREX_DATA.SILVER.VALIDATED_RATES
    SELECT 
        timestamp,
        base_currency,
        target_currency,
        rate,
        date,
        time
    FROM FOREX_DATA.BRONZE.RAW_RATES
    WHERE rate > 0
    AND base_currency IS NOT NULL
    AND target_currency IS NOT NULL
    """
    return "Silver layer updated successfully"

# Function to process gold layer
def gold_layer_processing(**context):
    # Transform silver to gold layer with business logic
    gold_query = """
    INSERT INTO FOREX_DATA.GOLD.FOREX_ANALYTICS
    SELECT 
        date,
        base_currency,
        target_currency,
        AVG(rate) as avg_rate,
        MAX(rate) as high_rate,
        MIN(rate) as low_rate,
        FIRST_VALUE(rate) OVER (PARTITION BY date, base_currency, target_currency ORDER BY timestamp) as open_rate,
        LAST_VALUE(rate) OVER (PARTITION BY date, base_currency, target_currency ORDER BY timestamp) as close_rate
    FROM FOREX_DATA.SILVER.VALIDATED_RATES
    GROUP BY date, base_currency, target_currency
    """
    return "Gold layer updated successfully"

# Function to update dashboard
def update_dashboard(**context):
    return "Dashboard updated successfully"

# Create tasks
fetch_rates_task = PythonOperator(
    task_id='fetch_forex_rates',
    python_callable=fetch_forex_rates,
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_and_save_data',
    python_callable=process_and_save_data,
    provide_context=True,
    dag=dag,
)

bronze_layer_task = PythonOperator(
    task_id='bronze_layer_processing',
    python_callable=bronze_layer_processing,
    provide_context=True,
    dag=dag,
)

silver_layer_task = PythonOperator(
    task_id='silver_layer_processing',
    python_callable=silver_layer_processing,
    provide_context=True,
    dag=dag,
)

gold_layer_task = PythonOperator(
    task_id='gold_layer_processing',
    python_callable=gold_layer_processing,
    provide_context=True,
    dag=dag,
)

update_dashboard_task = PythonOperator(
    task_id='update_dashboard',
    python_callable=update_dashboard,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_rates_task >> process_data_task >> bronze_layer_task >> silver_layer_task >> gold_layer_task >> update_dashboard_task 
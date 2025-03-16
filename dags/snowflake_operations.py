from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from data_warehouse.snowflake_connector import SnowflakeConnector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_snowflake_maintenance():
    """Run maintenance operations on Snowflake tables"""
    snowflake = SnowflakeConnector()
    # Example: Run VACUUM on tables to reclaim space
    snowflake.execute_query("ALTER TABLE realtime_rates EXECUTE VACUUM")
    snowflake.close()

with DAG(
    'snowflake_maintenance',
    default_args=default_args,
    description='Perform maintenance operations on Snowflake',
    schedule_interval=timedelta(days=1),
) as dag:
    
    maintenance_task = PythonOperator(
        task_id='run_snowflake_maintenance',
        python_callable=run_snowflake_maintenance,
    ) 
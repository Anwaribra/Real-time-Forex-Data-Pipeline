#!/bin/bash

# Load environment variables
if [ -f ".env" ]; then
    source .env
    echo "Environment variables loaded from .env"
fi

# Set environment variables
export PROJECT_ROOT=$(pwd)
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$PROJECT_ROOT:$PYTHONPATH

# Create Airflow directory if it doesn't exist
mkdir -p $AIRFLOW_HOME/dags

# Create symbolic link to our DAGs
ln -sf $PROJECT_ROOT/dags/dags/* $AIRFLOW_HOME/dags/

# Initialize Airflow DB if it doesn't exist
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db init
    
    # Create admin user
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
        
    # Configure Snowflake connection
    echo "Setting up Snowflake connection..."
    airflow connections add 'snowflake_conn' \
        --conn-type 'snowflake' \
        --conn-host "$SNOWFLAKE_ACCOUNT" \
        --conn-login "$SNOWFLAKE_USER" \
        --conn-password "$SNOWFLAKE_PASSWORD" \
        --conn-schema "$SNOWFLAKE_SCHEMA" \
        --conn-extra "{\"database\":\"$SNOWFLAKE_DATABASE\",\"warehouse\":\"$SNOWFLAKE_WAREHOUSE\",\"role\":\"$SNOWFLAKE_ROLE\",\"authenticator\":\"snowflake\"}"
fi

# Start Airflow webserver in the background
echo "Starting Airflow webserver..."
airflow webserver -p 8080 -D

# Wait for webserver to start
sleep 5

# Start Airflow scheduler in the background
echo "Starting Airflow scheduler..."
airflow scheduler -D

# Display information
echo ""
echo "Airflow is running!"
echo "Webserver: http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
echo ""
echo "DAGs available:"
airflow dags list

echo ""
echo "To trigger the forex pipeline manually, run:"
echo "airflow dags trigger forex_pipeline"
echo ""
echo "To stop Airflow:"
echo "pkill -f airflow" 
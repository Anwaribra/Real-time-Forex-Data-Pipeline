#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Export Airflow home
export AIRFLOW_HOME=~/airflow

# Start Airflow scheduler in background
echo "Starting Airflow scheduler..."
airflow scheduler > ~/airflow/logs/scheduler.log 2>&1 &
SCHEDULER_PID=$!

# Start Airflow webserver in background
echo "Starting Airflow webserver..."
airflow webserver -p 8080 > ~/airflow/logs/webserver.log 2>&1 &
WEBSERVER_PID=$!

echo "Airflow is running!"
echo "Access the web interface at http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
echo ""
echo "To stop Airflow, run: ./stop_airflow.sh"

# Save PIDs for stop script
echo $SCHEDULER_PID > ~/airflow/scheduler.pid
echo $WEBSERVER_PID > ~/airflow/webserver.pid

#!/bin/bash

# Kill processes
if [ -f ~/airflow/scheduler.pid ]; then
    kill $(cat ~/airflow/scheduler.pid)
    rm ~/airflow/scheduler.pid
fi

if [ -f ~/airflow/webserver.pid ]; then
    kill $(cat ~/airflow/webserver.pid)
    rm ~/airflow/webserver.pid
fi

echo "Airflow stopped"

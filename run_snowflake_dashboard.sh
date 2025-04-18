#!/bin/bash

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "Activated virtual environment"
else
    echo "No virtual environment found at .venv. Consider creating one with: python -m venv .venv"
fi

# Install required packages if needed
pip install -r requirements.txt

# Run the Snowflake Streamlit dashboard
echo "Starting Snowflake dashboard..."
streamlit run snowflake_dashboard.py 
#!/bin/bash

# Activate virtual environment if it exists
if [ -d "../.venv" ]; then
    source ../.venv/bin/activate
    echo "Activated virtual environment"
elif [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "Activated virtual environment"
else
    echo "No virtual environment found. Consider creating one with: python -m venv .venv"
fi

# Install required packages if needed
pip install -r ../requirements.txt

# Run the Streamlit dashboard
echo "Starting Streamlit dashboard..."
streamlit run dashboard.py 
#!/usr/bin/env python3
"""
View Latest Forex Rates
This script displays the latest forex rates stored in the system.
"""
import os
import json
from pathlib import Path
import pandas as pd

def get_latest_rates():
    """Find and display the latest rates from data directory"""
    # Search directory
    search_dir = Path(__file__).parent / 'data'
    
    if not search_dir.exists():
        print("Data directory not found")
        return None
        
    json_files = list(search_dir.glob('*forex_rates_*.json'))
    
    if not json_files:
        print("No forex rate files found in data directory")
        return None
        
    # Get the latest file by sorting based on modification time
    latest_file = max(json_files, key=os.path.getmtime)
    print(f"Latest data file: {latest_file}")
    
    # Load and return the data
    with open(latest_file, 'r') as f:
        return json.load(f)

def display_rates(data):
    """Display the rates in a formatted table"""
    if not data:
        print("No valid data found")
        return
    
    # Determine the data format and extract rates
    timestamp = data.get('timestamp', 'Unknown')
    
    # Handle different formats (our backup format vs original format)
    if 'data' in data:
        # Backup format
        rates = data['data']
        df = pd.DataFrame(rates)
    elif 'rates' in data:
        # Original format
        rates_dict = data['rates']
        rows = []
        for pair, rate in rates_dict.items():
            from_curr, to_curr = pair.split('_')
            rows.append({
                'from_currency': from_curr,
                'to_currency': to_curr,
                'exchange_rate': rate
            })
        df = pd.DataFrame(rows)
    else:
        print("Unknown data format")
        return
    
    # Print header
    print("\n===== LATEST FOREX RATES =====")
    print(f"Last Updated: {timestamp}")
    print("=" * 30)
    
    # Print each exchange rate
    for _, row in df.iterrows():
        from_curr = row['from_currency']
        to_curr = row['to_currency']
        rate = row['exchange_rate']
        print(f"{from_curr}/{to_curr}: {rate:.4f}")
    
    print("=" * 30)

if __name__ == "__main__":
    data = get_latest_rates()
    if data:
        display_rates(data)
    else:
        print("No data available. Run the pipeline first with: python run_pipeline.py") 
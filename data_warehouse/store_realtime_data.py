import json
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from data_warehouse.snowflake_connector import SnowflakeConnector


# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)




def store_realtime_data_to_snowflake(currency_pair, data_path=None, data=None):
    """
    Store real-time currency exchange rate data in Snowflake
    
    Args:
        currency_pair (str): Currency pair (e.g., 'USD_EGP')
        data_path (str, optional): Path to data file
        data (dict, optional): Exchange rate data directly
    """
    if data is None and data_path:
        # Read data from file
        with open(data_path, 'r') as file:
            data = json.load(file)
    
    if not data:
        print("No data to store")
        return
    
    # Convert data to DataFrame
    timestamp = datetime.now().isoformat()
    df = pd.DataFrame([{
        'timestamp': timestamp,
        'currency_pair': currency_pair,
        'rate': data.get('rate', 0),
        'bid': data.get('bid', 0),
        'ask': data.get('ask', 0)
    }])
    
    # Store data in Snowflake
    try:
        snowflake = SnowflakeConnector()
        table_name = "realtime_rates"
        snowflake.load_dataframe_to_table(df, table_name)
        snowflake.close()
        print(f"Successfully stored {currency_pair} data in Snowflake")
    except Exception as e:
        print(f"Error storing data in Snowflake: {e}") 
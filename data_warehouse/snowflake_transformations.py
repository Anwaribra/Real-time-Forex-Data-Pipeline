import sys
from pathlib import Path
# Now import using the full path
from data_warehouse.snowflake_connector import SnowflakeConnector
from data_warehouse.store_realtime_data import store_realtime_data_to_snowflake

from data_warehouse.snowflake_connector import SnowflakeConnector
# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)
def create_daily_aggregates():
    """
    Create daily aggregates of forex data in Snowflake
    """
    snowflake = SnowflakeConnector()
    
    query = """
    CREATE OR REPLACE TABLE daily_forex_aggregates AS
    SELECT 
        DATE_TRUNC('DAY', timestamp) as date,
        currency_pair,
        AVG(rate) as avg_rate,
        MIN(rate) as min_rate,
        MAX(rate) as max_rate,
        COUNT(*) as sample_count
    FROM realtime_rates
    GROUP BY date, currency_pair
    ORDER BY date DESC, currency_pair
    """
    
    snowflake.execute_query(query)
    snowflake.close() 
#!/usr/bin/env python3
"""
Test script for the forex data pipeline components
"""
import os
import sys
import logging
import pandas as pd
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_fetch_data():
    """Test data ingestion components"""
    logger.info("Testing forex data fetching...")
    
    try:
        from data_ingestion.fetch_data import fetch_forex_data
        
        # Test fetch current data
        logger.info("Fetching current forex rates...")
        df = fetch_forex_data()
        
        if df.empty:
            logger.error("❌ Failed: No data returned from fetch_forex_data()")
            return False
            
        logger.info(f"✅ Success: Fetched {len(df)} rows of current forex data")
        logger.info(f"Data sample:\n{df.head()}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed: Error in fetch_data test: {str(e)}")
        return False

def test_historical_data():
    """Test historical data fetching"""
    logger.info("Testing historical data fetching...")
    
    try:
        from data_ingestion.fetch_historical_data import fetch_historical_data
        
        # Test with a small number of days for quick testing
        logger.info("Fetching 2 days of historical data...")
        df = fetch_historical_data(days=2)
        
        if df.empty:
            logger.error("❌ Failed: No data returned from fetch_historical_data()")
            return False
            
        logger.info(f"✅ Success: Fetched {len(df)} rows of historical forex data")
        logger.info(f"Data sample:\n{df.head()}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed: Error in historical data test: {str(e)}")
        return False

def test_snowflake_connection():
    """Test Snowflake connection and data saving"""
    logger.info("Testing Snowflake connection...")
    
    try:
        from data_storage.save_to_snowflake import save_to_snowflake
        
        # Create test data
        test_data = {
            'from_currency': ['USD', 'EUR', 'GBP'],
            'to_currency': ['EUR', 'USD', 'USD'],
            'exchange_rate': [0.92, 1.09, 1.27],
            'last_refreshed': ['2024-03-18 12:00:00'] * 3,
            'timestamp': pd.to_datetime(['2024-03-18 12:00:00'] * 3)
        }
        test_df = pd.DataFrame(test_data)
        
        # Save to test table
        logger.info("Saving test data to Snowflake...")
        table_name = f"FOREX_TEST_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        success = save_to_snowflake(test_df, table_name=table_name)
        
        if not success:
            logger.error("❌ Failed: Could not save data to Snowflake")
            return False
            
        logger.info(f"✅ Success: Saved test data to Snowflake table {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed: Error in Snowflake test: {str(e)}")
        return False

def run_all_tests():
    """Run all pipeline tests"""
    logger.info("==== FOREX PIPELINE TEST SUITE ====")
    
    tests = [
        ("Data Fetching", test_fetch_data),
        ("Historical Data", test_historical_data),
        ("Snowflake Integration", test_snowflake_connection)
    ]
    
    results = {}
    
    for name, test_func in tests:
        logger.info(f"\n==== Testing {name} ====")
        result = test_func()
        results[name] = result
        
    # Print summary
    logger.info("\n==== TEST SUMMARY ====")
    all_passed = True
    
    for name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{name}: {status}")
        if not result:
            all_passed = False
            
    if all_passed:
        logger.info("\n🎉 All tests passed! Your pipeline is ready.")
        return 0
    else:
        logger.error("\n❌ Some tests failed. Please check the logs.")
        return 1

if __name__ == "__main__":
    sys.exit(run_all_tests()) 
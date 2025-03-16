from data_warehouse.snowflake_connector import SnowflakeConnector

def test_connection():
    try:
        snowflake = SnowflakeConnector()
        conn = snowflake.connect()
        print("Successfully connected to Snowflake!")
        
        # Test query
        cursor = snowflake.execute_query("SELECT current_version()")
        result = cursor.fetchone()
        print(f"Snowflake version: {result[0]}")
        
        snowflake.close()
        return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

if __name__ == "__main__":
    test_connection() 
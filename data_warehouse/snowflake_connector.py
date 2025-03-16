import json
import pandas as pd

try:
    import snowflake.connector
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False
    print("WARNING: Snowflake packages not installed. Snowflake functionality will be disabled.")

class SnowflakeConnector:
    def __init__(self, config_path='config/config.json'):
        """
        Initialize Snowflake connection using settings from the configuration file
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("Snowflake packages not installed")
            
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
            
        self.snowflake_config = config.get('snowflake', {})
        self.conn = None
        self.engine = None
        
    def connect(self):
        """
        Create a connection to Snowflake
        """
        try:
            self.conn = snowflake.connector.connect(
                account=self.snowflake_config.get('account'),
                user=self.snowflake_config.get('user'),
                password=self.snowflake_config.get('password'),
                warehouse=self.snowflake_config.get('warehouse'),
                database=self.snowflake_config.get('database'),
                schema=self.snowflake_config.get('schema')
            )
            return self.conn
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            raise
    
    def get_sqlalchemy_engine(self):
        """
        Create a SQLAlchemy engine for Snowflake operations
        """
        if not self.engine:
            connection_url = URL(
                account=self.snowflake_config.get('account'),
                user=self.snowflake_config.get('user'),
                password=self.snowflake_config.get('password'),
                database=self.snowflake_config.get('database'),
                schema=self.snowflake_config.get('schema'),
                warehouse=self.snowflake_config.get('warehouse')
            )
            self.engine = create_engine(connection_url)
        return self.engine
    
    def execute_query(self, query):
        """
        Execute a SQL query on Snowflake
        """
        if not self.conn:
            self.connect()
            
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
            return cursor
        except Exception as e:
            print(f"Error executing query: {e}")
            raise
    
    def load_dataframe_to_table(self, df, table_name, if_exists='append'):
        """
        Load a DataFrame to a Snowflake table
        
        Args:
            df (pandas.DataFrame): Data to be loaded
            table_name (str): Table name
            if_exists (str): How to handle existing table ('fail', 'replace', 'append')
        """
        engine = self.get_sqlalchemy_engine()
        try:
            df.to_sql(
                name=table_name,
                con=engine,
                index=False,
                if_exists=if_exists
            )
            print(f"Data successfully loaded to table {table_name}")
        except Exception as e:
            print(f"Error loading data to Snowflake: {e}")
            raise
    
    def close(self):
        """
        Close the Snowflake connection
        """
        if self.conn:
            self.conn.close()
            self.conn = None 
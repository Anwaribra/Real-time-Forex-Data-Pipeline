import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
import json
import time
from kafka import KafkaProducer, KafkaConsumer

from data_ingestion.fetch_data import fetch_forex_data
from data_ingestion.fetch_historical_data import fetch_historical_data
# from data_processing.transform_data import aggregate_yearly_data as transform_data
from data_storage.save_to_csv import process_all_forex_data

def setup_logging() -> None:
    """Configure logging settings."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )

def load_config():
    """Load configuration from config file."""
    with open("config/config.json", "r") as f:
        return json.load(f)

class ForexKafkaHandler:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=1000
        )
        self.forex_topic = 'forex_data'
        
    def send_forex_data(self, currency_pair: str, data: dict):
        """Send forex data to Kafka topic"""
        try:
            message = {
                'currency_pair': currency_pair,
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            self.producer.send(self.forex_topic, message)
            self.producer.flush()
            logging.info(f"Sent forex data for {currency_pair} to Kafka")
        except Exception as e:
            logging.error(f"Failed to send data to Kafka: {str(e)}")

    def process_forex_data(self):
        """Process forex data from Kafka topic"""
        consumer = KafkaConsumer(
            self.forex_topic,
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='forex_processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            session_timeout_ms=45000,
            heartbeat_interval_ms=15000
        )
        
        try:
            for message in consumer:
                data = message.value
                # Process the forex data
                currency_pair = data['currency_pair']
                forex_data = data['data']
                
                # Save to CSV or perform other processing
                self._save_processed_data(currency_pair, forex_data)
                
        except Exception as e:
            logging.error(f"Error processing Kafka messages: {str(e)}")
        finally:
            consumer.close()

    def _save_processed_data(self, currency_pair: str, data: dict):
        """Save processed forex data"""
        try:
            # Implementation for saving data
            processed_dir = Path("processed_data")
            processed_dir.mkdir(exist_ok=True)
            
            filename = f"{currency_pair}_{datetime.now().strftime('%Y%m%d')}.json"
            filepath = processed_dir / filename
            
            with open(filepath, 'a') as f:
                json.dump(data, f)
                f.write('\n')
                
            logging.info(f"Saved processed data for {currency_pair}")
        except Exception as e:
            logging.error(f"Failed to save processed data: {str(e)}")

def check_kafka_connection(bootstrap_servers):
    """Check if Kafka is available"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.close()
        return True
    except Exception as e:
        logging.error(f"Kafka connection failed: {str(e)}")
        return False

def run_pipeline(fetch_realtime: bool = True, fetch_historical: bool = True, 
                transform: bool = True) -> None:
    """
    Run the complete data pipeline with Kafka integration.
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting forex data pipeline")
    
    try:
        # Check Kafka connection before starting
        if not check_kafka_connection(['kafka:29092']):
            raise Exception("Cannot connect to Kafka broker")
            
        # Initialize Kafka handler
        kafka_handler = ForexKafkaHandler()
        
        # Load configuration
        config = load_config()
        currency_pairs = config["currency_pairs"]
        
        # Fetch and send real-time data to Kafka
        if fetch_realtime:
            logger.info("Fetching real-time forex data")
            for pair in currency_pairs:
                data = fetch_forex_data()  # Your existing fetch function
                if data:
                    kafka_handler.send_forex_data(pair, data)
        
        # Process data from Kafka
        if transform:
            logger.info("Processing forex data from Kafka")
            kafka_handler.process_forex_data()
        
        logger.info("Pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

def cleanup_old_files(days: Optional[int] = 7) -> None:
    """
    Clean up old data files.
    
    Args:
        days: Number of days to keep files (default: 7)
    """
    logger = logging.getLogger(__name__)
    
    try:
        from datetime import datetime, timedelta
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Clean up data files
        data_dir = Path("data")
        for file in data_dir.glob("*"):
            if file.stat().st_mtime < cutoff_date.timestamp():
                file.unlink()
                logger.info(f"Deleted old file: {file}")
                
        # Clean up processed files
        processed_dir = Path("processed_data")
        for file in processed_dir.glob("*"):
            if file.stat().st_mtime < cutoff_date.timestamp():
                file.unlink()
                logger.info(f"Deleted old processed file: {file}")
                
        # Clean up old logs
        logs_dir = Path("logs")
        for file in logs_dir.glob("pipeline_*.log"):
            if file.stat().st_mtime < cutoff_date.timestamp():
                file.unlink()
                logger.info(f"Deleted old log file: {file}")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")

# Producer example
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Consumer example
consumer = KafkaConsumer(
    'your_topic_name',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='your_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if __name__ == "__main__":
    # Set up logging
    setup_logging()
    
    try:
        # Run the complete pipeline
        run_pipeline(
            fetch_realtime=True,
            fetch_historical=True,
            transform=True
        )
        
        # Optional: Clean up old files
        cleanup_old_files(days=7)
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise

import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

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
        # Wait for Kafka to be ready
        time.sleep(10)
        
        self.bootstrap_servers = ['kafka:29092']
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=1000
        )
        self.forex_topic = 'forex_data'
        
        # Create topic if it doesn't exist
        self._ensure_topic_exists()
    
    def _ensure_topic_exists(self):
        """Ensure the forex topic exists"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='forex-admin'
            )
            
            try:
                admin_client.create_topics([NewTopic(
                    name=self.forex_topic,
                    num_partitions=1,
                    replication_factor=1
                )])
                logging.info(f"Created topic: {self.forex_topic}")
            except Exception as e:
                if "already exists" in str(e):
                    logging.info(f"Topic {self.forex_topic} already exists")
                else:
                    logging.error(f"Error creating topic: {str(e)}")
        finally:
            admin_client.close()

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

def test_kafka_connection():
    """Test Kafka producer and consumer"""
    logger = logging.getLogger(__name__)
    producer = None
    consumer = None
    admin_client = None
    
    try:
        # Wait for Kafka to be ready
        time.sleep(10)
        
        bootstrap_servers = ['kafka:29092']
        test_topic = 'test-topic'
        
        # Create admin client and test topic
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='test-admin'
        )
        
        try:
            admin_client.create_topics([NewTopic(
                name=test_topic,
                num_partitions=1,
                replication_factor=1
            )])
            logger.info(f"Created test topic: {test_topic}")
        except Exception as e:
            if "already exists" not in str(e):
                raise
            
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=1000
        )
        
        # Send test message
        test_message = {"test": "Hello Kafka!"}
        future = producer.send(test_topic, test_message)
        future.get(timeout=10)  # Wait for message to be sent
        producer.flush()
        logger.info("Test message sent successfully")
        
        # Test consumer
        consumer = KafkaConsumer(
            test_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='test-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        # Try to read the message
        message_received = False
        for message in consumer:
            logger.info(f"Received test message: {message.value}")
            message_received = True
            break
            
        if not message_received:
            logger.error("No test message received")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Kafka test failed: {str(e)}")
        return False
        
    finally:
        # Clean up resources
        if producer:
            producer.close()
        if consumer:
            consumer.close()
        if admin_client:
            admin_client.close()

if __name__ == "__main__":
    # Set up logging
    setup_logging()
    
    try:
        # Test Kafka connection first
        if not test_kafka_connection():
            raise Exception("Kafka connection test failed")
            
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

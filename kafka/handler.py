import logging
from pathlib import Path
from datetime import datetime
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

class ForexKafkaHandler:
    def __init__(self):
        self.bootstrap_servers = ['localhost:9092']
        self.producer = self._create_producer()
        self.forex_topic = 'forex_data'
        self._ensure_topic_exists()
    
    def _create_producer(self):
        """Create and return Kafka producer"""
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            retry_backoff_ms=1000
        )
    
    def _ensure_topic_exists(self):
        """Ensure the forex topic exists"""
        admin_client = None
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
                    raise
        finally:
            if admin_client:
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
            bootstrap_servers=self.bootstrap_servers,
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
                currency_pair = data['currency_pair']
                forex_data = data['data']
                self._save_processed_data(currency_pair, forex_data)
        except Exception as e:
            logging.error(f"Error processing Kafka messages: {str(e)}")
        finally:
            consumer.close()

    def _save_processed_data(self, currency_pair: str, data: dict):
        """Save processed forex data"""
        try:
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
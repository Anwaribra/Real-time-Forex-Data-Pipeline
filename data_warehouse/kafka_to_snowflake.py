import json
import sys
from pathlib import Path
from time import sleep
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from data_warehouse.store_realtime_data import store_realtime_data_to_snowflake

def create_kafka_consumer(max_retries=3, retry_delay=5):
    """Create Kafka consumer with retry logic"""
    with open('config/config.json', 'r') as f:
        config = json.load(f)
    
    kafka_config = config.get('kafka', {})
    bootstrap_servers = kafka_config.get('bootstrap_servers').split(',')
    topic = kafka_config.get('topic')
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='snowflake-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                security_protocol='PLAINTEXT',  # Explicitly set security protocol
                api_version=(0, 10, 1),  # Specify Kafka API version
                consumer_timeout_ms=1000  # Add timeout for testing
            )
            # Test the connection
            consumer.topics()
            print("Successfully connected to Kafka")
            return consumer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{max_retries}: No brokers available. Retrying in {retry_delay} seconds...")
            sleep(retry_delay)
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Error connecting to Kafka: {e}")
            sleep(retry_delay)
    
    raise Exception("Failed to connect to Kafka after multiple attempts")

def consume_and_store():
    """Consume messages from Kafka and store them in Snowflake"""
    try:
        consumer = create_kafka_consumer()
        print("Starting Kafka consumer... Waiting for messages")
        
        for message in consumer:
            try:
                data = message.value
                print(f"Received message: {data}")
                
                if "Realtime Currency Exchange Rate" in data:
                    exchange_data = data["Realtime Currency Exchange Rate"]
                    from_currency = exchange_data.get("1. From_Currency Code")
                    to_currency = exchange_data.get("3. To_Currency Code")
                    
                    if from_currency and to_currency:
                        currency_pair = f"{from_currency}_{to_currency}"
                        processed_data = {
                            'rate': float(exchange_data.get("5. Exchange Rate", 0)),
                            'bid': float(exchange_data.get("8. Bid Price", 0)),
                            'ask': float(exchange_data.get("9. Ask Price", 0))
                        }
                        store_realtime_data_to_snowflake(currency_pair, data=processed_data)
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    consume_and_store() 
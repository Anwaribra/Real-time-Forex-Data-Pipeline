from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from time import sleep

def test_kafka_connection(max_retries=3):
    """Test Kafka connection with retry logic"""
    
    # List of possible bootstrap server configurations
    bootstrap_configs = [
        ['localhost:9092'],
        ['kafka:29092'],
        ['localhost:9092', 'kafka:29092']
    ]
    
    for attempt in range(max_retries):
        for bootstrap_servers in bootstrap_configs:
            try:
                print(f"\nAttempt {attempt + 1}/{max_retries} with bootstrap_servers={bootstrap_servers}")
                
                # Test producer connection
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    security_protocol='PLAINTEXT',
                    api_version=(0, 10, 1)
                )
                print("✓ Successfully connected to Kafka as producer")
                
                # Send test message
                future = producer.send('forex_data', {'test': 'message'})
                result = future.get(timeout=10)  # Wait for confirmation
                producer.flush()
                print("✓ Successfully sent test message")
                
                # Test consumer connection
                consumer = KafkaConsumer(
                    'forex_data',
                    bootstrap_servers=bootstrap_servers,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id='test-group',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=5000,
                    security_protocol='PLAINTEXT',
                    api_version=(0, 10, 1)
                )
                print("✓ Successfully connected to Kafka as consumer")
                
                # Try to read the test message
                print("Attempting to read test message...")
                for message in consumer:
                    print(f"✓ Received test message: {message.value}")
                    return True
                
            except NoBrokersAvailable:
                print(f"✗ No brokers available at {bootstrap_servers}")
            except Exception as e:
                print(f"✗ Error with {bootstrap_servers}: {str(e)}")
            finally:
                if 'producer' in locals():
                    producer.close()
                if 'consumer' in locals():
                    consumer.close()
        
        if attempt < max_retries - 1:
            print(f"\nRetrying in 5 seconds...")
            sleep(5)
    
    return False

if __name__ == "__main__":
    # First, print some debug information
    print("=== Kafka Connection Test ===")
    print("This test will attempt to connect to Kafka using different configurations.")
    print("If you're running Kafka in Docker, make sure the containers are up.\n")
    
    success = test_kafka_connection()
    
    if not success:
        print("\n=== Troubleshooting Steps ===")
        print("1. Check if Kafka is running:")
        print("   $ docker ps | grep kafka")
        print("\n2. Check Kafka logs:")
        print("   $ docker logs kafka")
        print("\n3. Check if ports are accessible:")
        print("   $ nc -zv localhost 9092")
        print("\n4. Make sure your docker-compose.yaml has the correct configuration") 
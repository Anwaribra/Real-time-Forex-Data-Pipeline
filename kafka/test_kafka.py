from kafka import KafkaProducer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_kafka():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        test_message = {"test": "Hello Kafka!"}
        producer.send('test-topic', test_message)
        producer.flush()
        logger.info("Test message sent successfully")
        return True
        
    except Exception as e:
        logger.error(f"Kafka test failed: {str(e)}")
        return False
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    test_kafka() 
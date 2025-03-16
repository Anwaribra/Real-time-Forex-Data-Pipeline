from kafka import KafkaProducer
import json
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_test_forex_data():
    producer = None
    try:
        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Sample forex data
        test_data = {
            "Realtime Currency Exchange Rate": {
                "1. From_Currency Code": "EUR",
                "3. To_Currency Code": "USD",
                "5. Exchange Rate": "1.0876",
                "6. Last Refreshed": "2024-03-16 21:45:00",
                "8. Bid Price": "1.0875",
                "9. Ask Price": "1.0877"
            }
        }
        
        # Send message
        producer.send('forex_data', test_data)
        producer.flush()
        logger.info("Test forex data sent successfully")
        
        # Keep script running for a few seconds
        time.sleep(5)
        
    except Exception as e:
        logger.error(f"Error sending test data: {str(e)}")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    send_test_forex_data() 
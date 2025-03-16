import json
from kafka import KafkaConsumer
from data_warehouse.store_realtime_data import store_realtime_data_to_snowflake

def consume_forex_data():
    consumer = KafkaConsumer(
        'forex_data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='forex-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Starting Kafka consumer... Waiting for messages")
    
    for message in consumer:
        data = message.value
        print(f"Received message: {data}")
        
        # Extract currency pair from data
        if "Realtime Currency Exchange Rate" in data:
            exchange_data = data["Realtime Currency Exchange Rate"]
            from_currency = exchange_data.get("1. From_Currency Code")
            to_currency = exchange_data.get("3. To_Currency Code")
            
            if from_currency and to_currency:
                currency_pair = f"{from_currency}_{to_currency}"
                
                # Process and store in Snowflake
                processed_data = {
                    'rate': float(exchange_data.get("5. Exchange Rate", 0)),
                    'bid': float(exchange_data.get("8. Bid Price", 0)),
                    'ask': float(exchange_data.get("9. Ask Price", 0))
                }
                
                store_realtime_data_to_snowflake(currency_pair, data=processed_data)

if __name__ == "__main__":
    consume_forex_data() 
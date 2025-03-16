import os
import pandas as pd
import json
import logging
from pathlib import Path
from kafka import KafkaConsumer
from json import loads

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def save_data_to_csv():
    """
    Receive data from Kafka and save it to CSV files
    """
    logger = logging.getLogger(__name__)
    setup_logging()
    
    # Create output directory for processed data
    output_dir = Path("processed_data")
    output_dir.mkdir(exist_ok=True)
    
    # Setup Kafka Consumer
    consumer = KafkaConsumer(
        'forex_data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='forex_group',
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    
    try:
        logger.info("Starting to receive data from Kafka...")
        for message in consumer:
            data = message.value
            
            try:
                # Process realtime data
                if "Realtime Currency Exchange Rate" in data:
                    exchange_data = data["Realtime Currency Exchange Rate"]
                    df = pd.DataFrame([{
                        "From_Currency": exchange_data.get("1. From_Currency Code"),
                        "To_Currency": exchange_data.get("3. To_Currency Code"),
                        "Exchange_Rate": exchange_data.get("5. Exchange Rate"),
                        "Last_Refreshed": exchange_data.get("6. Last Refreshed"),
                        "Bid_Price": exchange_data.get("8. Bid Price"),
                        "Ask_Price": exchange_data.get("9. Ask Price")
                    }])
                    
                    pair = f"{exchange_data.get('1. From_Currency Code')}_{exchange_data.get('3. To_Currency Code')}"
                    output_file = output_dir / f"realtime_{pair}.csv"
                    df.to_csv(output_file, mode='a', header=not output_file.exists(), index=False)
                    logger.info(f"Saved realtime data: {output_file}")
                
                # Process historical data
                elif "Time Series FX (Daily)" in data:
                    df = pd.DataFrame.from_dict(
                        data["Time Series FX (Daily)"],
                        orient="index"
                    )
                    df.index.name = "Date"
                    
                    pair = data.get("Meta Data", {}).get("2. From Symbol", "") + "_" + \
                           data.get("Meta Data", {}).get("3. To Symbol", "")
                    output_file = output_dir / f"historical_{pair}.csv"
                    df.to_csv(output_file)
                    logger.info(f"Saved historical data: {output_file}")
            
            except Exception as e:
                logger.error(f"Error processing data: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {str(e)}")
        raise
    finally:
        consumer.close()

if __name__ == '__main__':
    save_data_to_csv()

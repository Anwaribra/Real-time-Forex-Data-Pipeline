import os
import pandas as pd
import json
import logging
from pathlib import Path

try:
    from kafka import KafkaConsumer
    from json import loads
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning("Kafka package not found. Kafka functionality will be disabled.")

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def process_all_forex_data(forex_data, historical_data):
    """
    Process and save forex data to CSV files
    """
    logger = logging.getLogger(__name__)
    setup_logging()
    
    # Create output directory for processed data
    output_dir = Path("processed_data")
    output_dir.mkdir(exist_ok=True)
    
    try:
        # Process and save forex data
        if forex_data:
            df = pd.DataFrame(forex_data)
            output_file = output_dir / "realtime_forex_data.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved realtime forex data to {output_file}")
        
        # Process and save historical data
        if historical_data:
            df = pd.DataFrame(historical_data)
            output_file = output_dir / "historical_forex_data.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Saved historical forex data to {output_file}")
    
    except Exception as e:
        logger.error(f"Error processing forex data: {str(e)}")
        raise

def save_data_to_csv():
    """
    Receive data and save it to CSV files
    """
    logger = logging.getLogger(__name__)
    setup_logging()
    
    # Create output directory for processed data
    output_dir = Path("processed_data")
    output_dir.mkdir(exist_ok=True)
    
    if not KAFKA_AVAILABLE:
        logger.error("Kafka functionality is not available. Please install kafka-python package.")
        return
    
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
        if 'consumer' in locals():
            consumer.close()

if __name__ == '__main__':
    save_data_to_csv()

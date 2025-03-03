from api_connector import PayPalAPI
from config import Config
import logging

logging.basicConfig(level=logging.INFO)

def main():
    
    print(f"Client ID length: {len(Config.PAYPAL_CLIENT_ID)}")
    print(f"Secret length: {len(Config.PAYPAL_CLIENT_SECRET)}")
    
    api = PayPalAPI(
        client_id=Config.PAYPAL_CLIENT_ID,
        client_secret=Config.PAYPAL_CLIENT_SECRET
    )
    
    if api.test_auth():
        print("PayPal connection successful!")
    else:
        print("PayPal connection failed!")

if __name__ == "__main__":
    main() 
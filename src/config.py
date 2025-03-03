import os
from dotenv import load_dotenv

class Config:
    
    load_dotenv()
    
    
    PAYPAL_CLIENT_ID = os.getenv('PAYPAL_CLIENT_ID')
    PAYPAL_CLIENT_SECRET = os.getenv('PAYPAL_CLIENT_SECRET')
    PAYPAL_SANDBOX = os.getenv('PAYPAL_SANDBOX', 'true').lower() == 'true'

    
    DB_HOST = os.getenv('DB_HOST', 'db')
    DB_USER = os.getenv('POSTGRES_USER', 'myuser')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'mypassword')
    DB_NAME = os.getenv('POSTGRES_DB', 'paypal_transactions')

    @classmethod
    def validate(cls):
        if not cls.PAYPAL_CLIENT_ID or not cls.PAYPAL_CLIENT_SECRET:
            raise ValueError("PayPal credentials not found in environment variables") 
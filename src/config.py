import os

class Config:
    # PayPal Configuration
    PAYPAL_CLIENT_ID = os.getenv('PAYPAL_CLIENT_ID')
    PAYPAL_CLIENT_SECRET = os.getenv('PAYPAL_CLIENT_SECRET')
    PAYPAL_SANDBOX = os.getenv('PAYPAL_SANDBOX', 'true').lower() == 'true'

    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'db')
    DB_USER = os.getenv('POSTGRES_USER', 'myuser')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'mypassword')
    DB_NAME = os.getenv('POSTGRES_DB', 'paypal_transactions') 
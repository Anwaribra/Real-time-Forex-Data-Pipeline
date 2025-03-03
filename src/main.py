from api_connector import PayPalAPI
from config import Config
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        
        paypal = PayPalAPI(
            client_id=Config.PAYPAL_CLIENT_ID,
            client_secret=Config.PAYPAL_CLIENT_SECRET
        )
        
        
        payment = paypal.create_payment(amount=10.00)
        
        
        approval_url = next(
            link["href"] for link in payment["links"] 
            if link["rel"] == "approval_url"
        )
        
        print("\ Payment Created!")
        print(f" Amount: $10.00")
        print(f" Approval URL: {approval_url}\n")
        
    except Exception as e:
        print(f"\n Error: {str(e)}\n")

if __name__ == "__main__":
    main()

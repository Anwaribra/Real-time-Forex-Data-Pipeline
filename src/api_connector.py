import requests
import logging

logger = logging.getLogger(__name__)

class PayPalAPI:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id.strip()
        self.client_secret = client_secret.strip()
        self.base_url = "https://api-m.sandbox.paypal.com"
        self.access_token = None

    def get_token(self):
        
        response = requests.post(
            f"{self.base_url}/v1/oauth2/token",
            auth=(self.client_id, self.client_secret),
            data={'grant_type': 'client_credentials'}
        )
        
        if response.ok:
            self.access_token = response.json()['access_token']
            return self.access_token
        raise Exception(f"Auth failed: {response.text}")

    def create_payment(self, amount: float):
        if not self.access_token:
            self.get_token()

        # Create payment
        response = requests.post(
            f"{self.base_url}/v1/payments/payment",
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.access_token}'
            },
            json={
                "intent": "sale",
                "payer": {"payment_method": "paypal"},
                "transactions": [{
                    "amount": {
                        "total": str(amount),
                        "currency": "USD"
                    }
                }],
                "redirect_urls": {
                    "return_url": "http://localhost:5000/success",
                    "cancel_url": "http://localhost:5000/cancel"
                }
            }
        )
        
        if response.ok:
            return response.json()
        raise Exception(f"Payment failed: {response.text}")

    def test_auth(self):
        """test the connection only"""
        response = requests.post(
            f"{self.base_url}/v1/oauth2/token",
            auth=(self.client_id, self.client_secret),
            data={'grant_type': 'client_credentials'}
        )
        
        if response.ok:
            data = response.json()
            logger.info(f"Connection successful! Token: {data['access_token'][:20]}...")
            return True
            
        logger.error(f"Auth failed: {response.text}")
        return False


import requests
import base64
import json
from typing import Dict, Optional

class PayPalAPIConnector:
    def __init__(self, client_id: str, client_secret: str, sandbox: bool = True):
        """
        Initialize PayPal API connector
        
        Args:
            client_id (str): PayPal Client ID
            client_secret (str): PayPal Client Secret
            sandbox (bool): Use sandbox environment if True, production if False
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = "https://api-m.sandbox.paypal.com" if sandbox else "https://api-m.paypal.com"
        self.access_token = None

    def get_access_token(self) -> str:
        """
        Get OAuth 2.0 access token from PayPal
        
        Returns:
            str: Access token
        """
        auth = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {"grant_type": "client_credentials"}

        response = requests.post(
            f"{self.base_url}/v1/oauth2/token",
            headers=headers,
            data=data
        )

        if response.status_code == 200:
            self.access_token = response.json()["access_token"]
            return self.access_token
        else:
            raise Exception(f"Failed to get access token: {response.text}")

    def create_payment(self, amount: float, currency: str = "USD", description: str = "") -> Dict:
        """
        Create a PayPal payment
        
        Args:
            amount (float): Payment amount
            currency (str): Currency code (default: USD)
            description (str): Payment description
            
        Returns:
            dict: Payment creation response
        """
        if not self.access_token:
            self.get_access_token()

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "intent": "sale",
            "payer": {
                "payment_method": "paypal"
            },
            "transactions": [{
                "amount": {
                    "total": str(amount),
                    "currency": currency
                },
                "description": description
            }],
            "redirect_urls": {
                "return_url": "http://your-website.com/success",  # Replace with your success URL
                "cancel_url": "http://your-website.com/cancel"    # Replace with your cancel URL
            }
        }

        response = requests.post(
            f"{self.base_url}/v1/payments/payment",
            headers=headers,
            data=json.dumps(payload)
        )

        if response.status_code in [200, 201]:
            return response.json()
        else:
            raise Exception(f"Payment creation failed: {response.text}")

    def execute_payment(self, payment_id: str, payer_id: str) -> Dict:
        """
        Execute an approved PayPal payment
        
        Args:
            payment_id (str): Payment ID from PayPal
            payer_id (str): Payer ID from PayPal
            
        Returns:
            dict: Payment execution response
        """
        if not self.access_token:
            self.get_access_token()

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "payer_id": payer_id
        }

        response = requests.post(
            f"{self.base_url}/v1/payments/payment/{payment_id}/execute",
            headers=headers,
            data=json.dumps(payload)
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Payment execution failed: {response.text}")

    def get_payment_details(self, payment_id: str) -> Dict:
        """
        Get details of a specific payment
        
        Args:
            payment_id (str): Payment ID from PayPal
            
        Returns:
            dict: Payment details
        """
        if not self.access_token:
            self.get_access_token()

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

        response = requests.get(
            f"{self.base_url}/v1/payments/payment/{payment_id}",
            headers=headers
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get payment details: {response.text}")


import requests
import base64
from config import Config

def test_auth():
    
    credentials = f"{Config.PAYPAL_CLIENT_ID}:{Config.PAYPAL_CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    url = "https://api-m.sandbox.paypal.com/v1/oauth2/token"
    headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Accept': 'application/json',
        'Accept-Language': 'en_US'
    }
    data = {'grant_type': 'client_credentials'}
    
    print("\nTesting PayPal Auth...")
    print(f"URL: {url}")
    print(f"Client ID: {Config.PAYPAL_CLIENT_ID[:10]}...")  
    print(f"Headers: {headers}")
    
    try:
        response = requests.post(url, headers=headers, data=data)
        print(f"\nStatus: {response.status_code}")
        print(f"Response: {response.text}\n")
        
        if response.ok:
            print("Connection successful!")
            return True
        else:
            print(" Connection failed!")
            return False
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    test_auth() 
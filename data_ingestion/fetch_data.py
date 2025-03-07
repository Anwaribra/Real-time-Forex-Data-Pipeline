import requests
import json
import os

def fetch_forex_data():
    # Load config file
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.json')
    
    with open(config_path, 'r') as file:
        config = json.load(file)

    base_url = config['forex_api_url']
    api_key = config['forex_api_key']
    currency_pairs = config['currency_pairs']

    # Ensure 'data/' directory exists
    data_dir = os.path.join(os.path.dirname(__file__), '../data')
    os.makedirs(data_dir, exist_ok=True)

    for pair in currency_pairs:
        from_currency, to_currency = pair.split('/')
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": from_currency,
            "to_currency": to_currency,
            "apikey": api_key
        }

        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            print(f"✅ Data fetched for {pair}")
            
            # Save data to JSON file
            filename = f"realtime_{from_currency}_{to_currency}.json"
            filepath = os.path.join(data_dir, filename)
            
            with open(filepath, "w") as file:
                json.dump(data, file, indent=4)

            print(f"💾 Saved to {filepath}")

        else:
            print(f"❌ Failed to fetch data for {pair}: {response.status_code} - {response.text}")

if __name__ == '__main__':
    fetch_forex_data()

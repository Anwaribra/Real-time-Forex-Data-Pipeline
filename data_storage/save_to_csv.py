import os
import pandas as pd
import json

# Ensure output directories exist
os.makedirs("processed_data", exist_ok=True)

def save_realtime_to_csv(data, pair):
    """Save real-time exchange rate data to CSV."""
    if "Realtime Currency Exchange Rate" not in data:
        print(f"⚠️ No real-time data available for {pair}")
        return
    
    # Extract relevant fields
    exchange_data = data["Realtime Currency Exchange Rate"]
    structured_data = {
        "From_Currency": exchange_data["1. From_Currency Code"],
        "To_Currency": exchange_data["3. To_Currency Code"],
        "Exchange Rate": exchange_data["5. Exchange Rate"],
        "Last Refreshed": exchange_data["6. Last Refreshed"],
        "Bid Price": exchange_data["8. Bid Price"],
        "Ask Price": exchange_data["9. Ask Price"]
    }

    df = pd.DataFrame([structured_data])
    filename = f"processed_data/realtime_{pair.replace('/', '_')}.csv"

    # Append or create new CSV
    df.to_csv(filename, mode="a", index=False, header=not os.path.exists(filename))
    print(f"✅ Real-time data saved to {filename}")

def save_historical_to_csv(data, pair):
    """Save historical exchange rate data to CSV."""
    if "Time Series FX (Daily)" not in data:
        print(f"⚠️ No historical data available for {pair}")
        return
    
    # Convert nested JSON to DataFrame
    df = pd.DataFrame.from_dict(data["Time Series FX (Daily)"], orient="index")
    df.index.name = "Date"

    # Rename columns for better readability
    df.rename(columns={
        "1. open": "Open",
        "2. high": "High",
        "3. low": "Low",
        "4. close": "Close"
    }, inplace=True)

    filename = f"processed_data/historical_{pair.replace('/', '_')}.csv"
    df.to_csv(filename)
    print(f"✅ Historical data saved to {filename}")

# Example usage
with open("data/realtime_EUR_USD.json", "r") as file:
    realtime_data = json.load(file)
    save_realtime_to_csv(realtime_data, "EUR/USD")

with open("data/historical_EUR_USD.json", "r") as file:
    historical_data = json.load(file)
    save_historical_to_csv(historical_data, "EUR/USD")

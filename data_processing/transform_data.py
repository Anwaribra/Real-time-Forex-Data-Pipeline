# import os
# import json
# import pandas as pd


# os.makedirs("processed_data", exist_ok=True)

# def load_historical_data(currency_pair):
#     file_path = f"data/historical_{currency_pair}.json"
#     if not os.path.exists(file_path):
#         raise FileNotFoundError(f"No data found for {currency_pair}. Please fetch data first.")
    
#     with open(file_path, "r") as file:
#         data = json.load(file)
    
#     return data

# def transform_historical_data(currency_pair, data):
#     """
#     Transform the historical data into a structured DataFrame.
#     """

#     records = []

#     time_series = data.get("Time Series FX (Daily)", {})
#     for date, values in time_series.items():
#         record = {
#             "currency_pair": currency_pair,
#             "date": date,
#             "open": float(values["1. open"]),
#             "high": float(values["2. high"]),
#             "low": float(values["3. low"]),
#             "close": float(values["4. close"])
#         }
#         records.append(record)
    
#     df = pd.DataFrame(records)
#     return df

# def save_transformed_data(currency_pair, df):
#     file_path = f"processed_data/transformed_{currency_pair}.csv"
#     df.to_csv(file_path, index=False)
#     print(f"Transformed data saved to {file_path}")

# def main():
#     currency_pairs = ["EUR/USD", "GBP/USD", "USD/JPY"]

#     for pair in currency_pairs:
#         try:
#             print(f" Transforming data for {pair}")
#             data = load_historical_data(pair.replace("/", "_"))
#             df = transform_historical_data(pair, data)
#             save_transformed_data(pair.replace("/", "_"), df)
#         except FileNotFoundError as e:
#             print(f" {e}")
#         except Exception as e:
#             print(f" Unexpected error for {pair}: {e}")

# if __name__ == "__main__":
#     main()

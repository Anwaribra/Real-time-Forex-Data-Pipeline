# import os
# import json
# import pandas as pd
# import logging
# from pathlib import Path
# from typing import Optional

# def setup_logging() -> None:
#     """Configure logging settings."""
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s',
#         handlers=[
#             logging.FileHandler('data_processing.log'),
#             logging.StreamHandler()
#         ]
#     )

# def load_historical_data(currency_pair: str) -> dict:
#     """Load historical data from JSON file."""
#     file_path = Path(f"data/historical_{currency_pair}.json")
#     if not file_path.exists():
#         raise FileNotFoundError(f"No data found for {currency_pair}. Please fetch data first.")
    
#     with open(file_path, "r") as file:
#         data = json.load(file)
    
#     return data

# def transform_historical_data(currency_pair: str, data: dict) -> pd.DataFrame:
#     """Transform the historical data into a structured DataFrame."""
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

# def save_transformed_data(currency_pair: str, df: pd.DataFrame) -> None:
#     """Save transformed data to CSV file."""
#     output_dir = Path("processed_data")
#     output_dir.mkdir(exist_ok=True)
    
#     file_path = output_dir / f"transformed_{currency_pair}.csv"
#     df.to_csv(file_path, index=False)
#     logging.info(f"Transformed data saved to {file_path}")

# def aggregate_yearly_data(input_dir: Optional[Path] = None, output_dir: Optional[Path] = None) -> None:
#     """Aggregate daily historical data to yearly summaries."""
#     logger = logging.getLogger(__name__)
    
#     input_dir = input_dir or Path("processed_data")
#     output_dir = output_dir or Path("processed_data/yearly")
#     output_dir.mkdir(parents=True, exist_ok=True)
    
#     # Get current year
#     current_year = pd.Timestamp.now().year
    
#     try:
#         # Process all transformed CSV files
#         for file in input_dir.glob("transformed_*.csv"):
#             try:
#                 # Read the data
#                 df = pd.read_csv(file)
#                 df['date'] = pd.to_datetime(df['date'])
#                 pair = file.stem.replace("transformed_", "")
                
#                 # Filter out future dates and sort
#                 df = df[df['date'].dt.year < current_year]
#                 df = df.sort_values('date')
                
#                 # Create yearly aggregations
#                 yearly_data = df.groupby(df['date'].dt.year).agg({
#                     'open': 'first',
#                     'high': 'max',
#                     'low': 'min',
#                     'close': ['last', 'mean', 'std']
#                 }).round(4)
                
#                 # Flatten column names
#                 yearly_data.columns = [
#                     'Year_Open',
#                     'Year_High',
#                     'Year_Low',
#                     'Year_Close',
#                     'Avg_Price',
#                     'Price_Volatility'
#                 ]
                
#                 # Validate data (check for unrealistic changes)
#                 yearly_data['YoY_Change'] = yearly_data['Year_Close'].pct_change() * 100
#                 suspicious_years = yearly_data[abs(yearly_data['YoY_Change']) > 30].index
#                 if len(suspicious_years) > 0:
#                     logger.warning(f"Suspicious price changes detected for {pair} in years: {suspicious_years.tolist()}")
                
#                 # Calculate yearly change
#                 yearly_data['Yearly_Change_%'] = (
#                     (yearly_data['Year_Close'] - yearly_data['Year_Open']) / 
#                     yearly_data['Year_Open'] * 100
#                 ).round(2)
                
#                 # Calculate trading range
#                 yearly_data['Trading_Range_%'] = (
#                     (yearly_data['Year_High'] - yearly_data['Year_Low']) /
#                     yearly_data['Year_Low'] * 100
#                 ).round(2)
                
#                 # Drop temporary column
#                 yearly_data = yearly_data.drop('YoY_Change', axis=1)
                
#                 # Save yearly analysis
#                 output_file = output_dir / f"yearly_{pair}.csv"
#                 yearly_data.to_csv(output_file)
#                 logger.info(f"Created yearly summary for {pair}")
                
#                 # Generate summary statistics
#                 summary = pd.DataFrame({
#                     'Metric': [
#                         'Currency Pair',
#                         'Date Range',
#                         'Total Years',
#                         'Average Yearly Change (%)',
#                         'Highest Yearly Change (%)',
#                         'Lowest Yearly Change (%)',
#                         'Most Volatile Year',
#                         'Highest Price',
#                         'Lowest Price',
#                         'Average Trading Range (%)'
#                     ],
#                     'Value': [
#                         pair,
#                         f"{df['date'].min().year} - {df['date'].max().year}",
#                         len(yearly_data),
#                         f"{yearly_data['Yearly_Change_%'].mean():.2f}%",
#                         f"{yearly_data['Yearly_Change_%'].max():.2f}%",
#                         f"{yearly_data['Yearly_Change_%'].min():.2f}%",
#                         str(yearly_data['Price_Volatility'].idxmax()),
#                         f"{yearly_data['Year_High'].max():.4f}",
#                         f"{yearly_data['Year_Low'].min():.4f}",
#                         f"{yearly_data['Trading_Range_%'].mean():.2f}%"
#                     ]
#                 })
                
#                 # Save summary
#                 summary_file = output_dir / f"summary_{pair}.csv"
#                 summary.to_csv(summary_file, index=False)
#                 logger.info(f"Created summary statistics for {pair}")
                
#             except Exception as e:
#                 logger.error(f"Error processing {file}: {str(e)}")
#                 continue
                
#     except Exception as e:
#         logger.error(f"Error in yearly aggregation: {str(e)}")
#         raise

# def process_historical_data(currency_pair: str, df: pd.DataFrame) -> pd.DataFrame:
#     """Process historical data and add additional metrics."""
#     # Sort by date
#     df = df.sort_index()
    
#     # Add year column
#     df['Year'] = pd.to_datetime(df.index).year
    
#     # Calculate yearly metrics
#     yearly_data = df.groupby('Year').agg({
#         'Open': 'first',
#         'High': 'max',
#         'Low': 'min',
#         'Close': ['last', 'mean', 'std']
#     })
    
#     # Flatten column names
#     yearly_data.columns = [
#         'Year_Open',
#         'Year_High', 
#         'Year_Low',
#         'Year_Close',
#         'Year_Avg',
#         'Year_Volatility'
#     ]
    
#     # Calculate yearly change
#     yearly_data['Yearly_Change_%'] = (
#         (yearly_data['Year_Close'] - yearly_data['Year_Open']) / 
#         yearly_data['Year_Open'] * 100
#     ).round(2)
    
#     # Calculate additional metrics
#     yearly_data['Trading_Range_%'] = (
#         (yearly_data['Year_High'] - yearly_data['Year_Low']) /
#         yearly_data['Year_Low'] * 100
#     ).round(2)
    
#     return yearly_data

# def generate_summary_stats(yearly_data: pd.DataFrame, currency_pair: str) -> pd.DataFrame:
#     """Generate summary statistics for the currency pair."""
#     summary = pd.DataFrame({
#         'Metric': [
#             'Currency Pair',
#             'Date Range',
#             'Total Years',
#             'Average Yearly Change (%)',
#             'Highest Yearly Change (%)',
#             'Lowest Yearly Change (%)', 
#             'Most Volatile Year',
#             'Highest Price',
#             'Lowest Price',
#             'Average Trading Range (%)'
#         ],
#         'Value': [
#             currency_pair,
#             f"{yearly_data.index.min()} - {yearly_data.index.max()}",
#             len(yearly_data),
#             f"{yearly_data['Yearly_Change_%'].mean():.2f}%",
#             f"{yearly_data['Yearly_Change_%'].max():.2f}%",
#             f"{yearly_data['Yearly_Change_%'].min():.2f}%",
#             str(yearly_data['Year_Volatility'].idxmax()),
#             f"{yearly_data['Year_High'].max():.4f}",
#             f"{yearly_data['Year_Low'].min():.4f}",
#             f"{yearly_data['Trading_Range_%'].mean():.2f}%"
#         ]
#     })
    
#     return summary

# def save_analysis(yearly_data: pd.DataFrame, summary: pd.DataFrame, 
#                  currency_pair: str, output_dir: Path) -> None:
#     """Save yearly analysis and summary to CSV files."""
#     # Save yearly analysis
#     yearly_file = output_dir / f"yearly_analysis_{currency_pair}.csv"
#     yearly_data.to_csv(yearly_file)
    
#     # Save summary
#     summary_file = output_dir / f"summary_{currency_pair}.csv"
#     summary.to_csv(summary_file, index=False)

# def main():
#     """Main function to process all currency pairs."""
#     setup_logging()
#     logger = logging.getLogger(__name__)
    
#     try:
#         config_path = Path("config/config.json")
#         with open(config_path, "r") as file:
#             config = json.load(file)
        
#         currency_pairs = config["currency_pairs"]
        
#         for pair in currency_pairs:
#             try:
#                 logger.info(f"Processing data for {pair}")
#                 data = load_historical_data(pair.replace("/", "_"))
#                 df = transform_historical_data(pair, data)
#                 save_transformed_data(pair.replace("/", "_"), df)
#             except Exception as e:
#                 logger.error(f"Error processing {pair}: {str(e)}")
    
#         # Generate yearly summaries
#         aggregate_yearly_data()
#         logger.info("Data processing completed successfully")
        
#     except Exception as e:
#         logger.error(f"Fatal error in main: {str(e)}")
#         raise

# if __name__ == "__main__":
#     main()

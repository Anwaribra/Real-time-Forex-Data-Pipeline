from data_ingestion.fetch_data import fetch_forex_data
from data_processing.transform_data import transform_data
# from data_storage.save_to_postgres import save_data_to_postgres

if __name__ == "__main__":
    raw_data = fetch_forex_data()
    cleaned_data = transform_data(raw_data)
    # save_data_to_postgres(cleaned_data)

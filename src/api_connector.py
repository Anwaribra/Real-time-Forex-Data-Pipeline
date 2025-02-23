import requests
import psycopg2
import os


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mypassword")
DB_NAME = os.getenv("DB_NAME", "real_time_data")
API_URL = "https://api.example.com/data"


conn = psycopg2.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    dbname=DB_NAME
)
# cursor = conn.cursor()

# def fetch_and_store_data():
#     response = requests.get(API_URL)
#     if response.status_code == 200:
#         data = response.json()

        
#         for item in data:
#             cursor.execute(
#                 "INSERT INTO data_table (id, value, timestamp) VALUES (%s, %s, %s)",
#                 (item["id"], item["value"], item["timestamp"])
#             )
#         conn.commit()
#     else:
#         print("Error fetching data:", response.status_code)

# if __name__ == "__main__":
#     fetch_and_store_data()

git clone https://github.com/hewan00/ETL_Pipeline_Project.git
cd ETL_Pipeline_Project

import pandas as pd
import requests
import os
import json
import sqlite3

def fetch_data_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()  # Assuming JSON data is returned
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from URL: {e}")
        return None

def load_json_to_csv(json_data, output_file):
    try:
        df = pd.json_normalize(json_data)
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")
        return df
    except Exception as e:
        print(f"Error converting JSON to CSV: {e}")
        return None

def load_csv_to_sqlite(csv_file, db_name):
    try:
        conn = sqlite3.connect(db_name)
        df = pd.read_csv(csv_file)
        df.to_sql('data_table', conn, if_exists='replace', index=False)
        print(f"Data loaded into SQLite database '{db_name}' in table 'data_table'.")
        return df
    except Exception as e:
        print(f"Error loading CSV to SQLite: {e}")
        return None

def summarize_data(df):
    return {
        "num_records": df.shape[0],
        "num_columns": df.shape[1]
    }

# Main function
if __name__ == "__main__":
    # Fetch data from a remote API or URL
    json_url = "https://jsonplaceholder.typicode.com/posts"  # Example API
    json_data = fetch_data_from_url(json_url)

    # If data fetched successfully, process it
    if json_data:
        csv_output = "output_data.csv"
        df_json = load_json_to_csv(json_data, csv_output)
        if df_json is not None:
            json_summary = summarize_data(df_json)
            print("JSON Summary:", json_summary)

            # Load CSV data into SQLite
            sqlite_db = "data.db"
            df_csv = load_csv_to_sqlite(csv_output, sqlite_db)
            if df_csv is not None:
                csv_summary = summarize_data(df_csv)
                print("CSV Summary:", csv_summary)


import tarfile
import json
import gzip
import pandas as pd
import os

# Function to extract and read NDJSON from a tar.gz file containing a JSON file
def load_tar_ndjson_to_df(tar_gz_file, json_filename):
    with tarfile.open(tar_gz_file, "r:gz") as tar:
        json_file = tar.extractfile(json_filename)
        data = [json.loads(line) for line in json_file if line.strip()]
    return pd.DataFrame(data)

# Function to extract and read NDJSON from a gzip file
def load_gzip_ndjson_to_df(gz_file):
    with gzip.open(gz_file, "rt", encoding="utf-8") as f:
        data = [json.loads(line) for line in f if line.strip()]
    return pd.DataFrame(data)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data")

users_file = os.path.join(DATA_PATH, "users.json.gz")
brands_file = os.path.join(DATA_PATH, "brands.json.gz")
receipts_file = os.path.join(DATA_PATH, "receipts.json.gz")

# Load the data
df_users = load_tar_ndjson_to_df(users_file, "users.json")
df_brands = load_gzip_ndjson_to_df(brands_file)
df_receipts = load_gzip_ndjson_to_df(receipts_file)

# Save the data to CSV files
df_users.to_csv(os.path.join(DATA_PATH, "users_raw.csv"), index=False)
df_brands.to_csv(os.path.join(DATA_PATH, "brands_raw.csv"), index=False)
df_receipts.to_csv(os.path.join(DATA_PATH, "receipts_raw.csv"), index=False)

print("users_raw.csv, brands_raw.csv, and receipts_raw.csv have been generated.")

# Print data overview
#print("Users DataFrame:")
#print(df_users.info(), "\n")
#print(df_users.head(), "\n")

#print("Brands DataFrame:")
#print(df_brands.info(), "\n")
#print(df_brands.head(), "\n")

#print("Receipts DataFrame:")
#print(df_receipts.info(), "\n")
#print(df_receipts.head(), "\n")

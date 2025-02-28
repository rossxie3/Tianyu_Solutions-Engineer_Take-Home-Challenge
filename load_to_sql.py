import os
import pandas as pd
import sqlite3

# Database file path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "fetch_data.db")
DATA_PATH = os.path.join(BASE_DIR, "data", "cleaned")

# Connect to the SQLite database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create database tables
cursor.executescript("""
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    _id TEXT PRIMARY KEY,
    active BOOLEAN,
    createdDate TIMESTAMP,
    lastLogin TIMESTAMP,
    role TEXT,
    signUpSource TEXT,
    state TEXT
);

DROP TABLE IF EXISTS brands;
CREATE TABLE brands (
    _id TEXT PRIMARY KEY,
    barcode TEXT,
    brandCode TEXT,
    category TEXT,
    categoryCode TEXT,
    cpg TEXT,
    topBrand BOOLEAN,
    name TEXT
);

DROP TABLE IF EXISTS receipts;
CREATE TABLE receipts (
    _id TEXT PRIMARY KEY,
    userId TEXT,
    createDate TIMESTAMP,
    dateScanned TIMESTAMP,
    finishedDate TIMESTAMP,
    modifyDate TIMESTAMP,
    pointsAwardedDate TIMESTAMP,
    purchaseDate TIMESTAMP,
    purchasedItemCount INTEGER,
    rewardsReceiptStatus TEXT,
    totalSpent REAL
);

DROP TABLE IF EXISTS receiptItems;
CREATE TABLE receiptItems (
    _id TEXT PRIMARY KEY,
    receiptId TEXT REFERENCES receipts(_id),
    brandCode TEXT REFERENCES brands(brandCode),
    barcode TEXT,
    brandName TEXT,
    description TEXT,
    quantity INTEGER,
    price REAL,
    isBonus BOOLEAN,
    needsFetchReview BOOLEAN
);
""")

# Load CSV files and insert into the database
df_users = pd.read_csv(os.path.join(DATA_PATH, "users_cleaned.csv"))
df_brands = pd.read_csv(os.path.join(DATA_PATH, "brands_cleaned.csv"))
df_receipts = pd.read_csv(os.path.join(DATA_PATH, "receipts_cleaned.csv"))
df_receipt_items = pd.read_csv(os.path.join(DATA_PATH, "receiptItems_cleaned.csv"))

df_users.to_sql("users", conn, if_exists="replace", index=False)
df_brands.to_sql("brands", conn, if_exists="replace", index=False)
df_receipts.to_sql("receipts", conn, if_exists="replace", index=False)
df_receipt_items.to_sql("receiptItems", conn, if_exists="replace", index=False)

# Commit and close the connection
conn.commit()
conn.close()

print("Data has been successfully inserted into the SQLite database 'fetch_data.db'.")

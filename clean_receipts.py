import os
import pandas as pd
from datetime import datetime

# Data paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(BASE_DIR, "data")
cleaned_data_path = os.path.join(data_path, "cleaned")

# Load the raw receipts CSV file
df_receipts = pd.read_csv(os.path.join(data_path, "receipts_raw.csv"))

# Print the current DataFrame columns
print("Current DataFrame columns:", df_receipts.columns)


# 1. Parse `_id`
def extract_oid(oid_field):
    """
    Extract the ObjectId from a _id field.
    
    If the field is a string containing the pattern "{'$oid': ...}",
    extract the actual oid value.
    """
    if isinstance(oid_field, str) and "{'$oid':" in oid_field:
        return oid_field.split(": '")[1].strip("'}")
    return oid_field

df_receipts["_id"] = df_receipts["_id"].apply(extract_oid)

# 2. Parse time fields
def extract_timestamp(date_field):
    """
    Parse a date field and convert it to 'YYYY-MM-DD HH:MM:SS' format.
    
    If the date field is in JSON format (e.g., "{'$date': ...}"),
    convert the milliseconds to seconds and then format the timestamp.
    If parsing fails, return None.
    """
    if isinstance(date_field, str) and "{'$date':" in date_field:
        try:
            timestamp = int(date_field.split(": ")[1].strip("}")) / 1000
            return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    return None if pd.isna(date_field) or date_field == "" else date_field

time_columns = ["createDate", "dateScanned", "finishedDate", "modifyDate", "pointsAwardedDate", "purchaseDate"]
for col in time_columns:
    if col in df_receipts.columns:
        df_receipts[col] = df_receipts[col].apply(extract_timestamp)

# 3. Fill missing values with "NULL"
df_receipts.fillna("NULL", inplace=True)


# Sort by 'dateScanned' (most recent first) to ensure we keep the latest records.
df_receipts.sort_values(by="dateScanned", ascending=False, inplace=True)
# Remove duplicates based on `_id`, keeping the latest record.
df_receipts.drop_duplicates(subset=["_id"], keep="first", inplace=True)
# Further de-duplicate submissions with the same `_id` and rewardsReceiptStatus within a short time span.
df_receipts.sort_values(by=["_id", "dateScanned"], inplace=True)
df_receipts.drop_duplicates(subset=["_id", "rewardsReceiptStatus"], keep="first", inplace=True)

# Display the processed receipts data overview
print("Processed receipts data overview:")
print(df_receipts.info())
print(df_receipts.head(10))  

# Save the cleaned DataFrame as a CSV file
df_receipts.to_csv(os.path.join(cleaned_data_path, "receipts_cleaned.csv"), index=False, na_rep="NULL")

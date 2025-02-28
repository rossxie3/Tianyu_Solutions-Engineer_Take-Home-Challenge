import os
import pandas as pd
from datetime import datetime


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(BASE_DIR, "data")
cleaned_data_path = os.path.join(data_path, "cleaned")

# Load the raw users CSV file
df_users = pd.read_csv(os.path.join(data_path, "users_raw.csv"))

# 1. Parse the `_id`
def extract_oid(oid_field):
    """
    Extract the ObjectId from a _id field.
    If the field contains the pattern "{'$oid': ...}",
    return the actual oid value.
    """
    if isinstance(oid_field, str) and "{'$oid':" in oid_field:
        return oid_field.split(": '")[1].strip("'}")
    return oid_field

df_users["_id"] = df_users["_id"].apply(extract_oid)

# 2. Parse timestamps
def extract_timestamp(date_field):
    """
    Parse a date field and convert it to the format 'YYYY-MM-DD HH:MM:SS'.
    If the date field is in the format "{'$date': ...}", convert the milliseconds to seconds.
    Return None if parsing fails.
    """
    if isinstance(date_field, str) and "{'$date':" in date_field:
        try:
            timestamp = int(date_field.split(": ")[1].strip("}")) / 1000
            return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    return None if pd.isna(date_field) or date_field == "" else date_field

df_users["createdDate"] = df_users["createdDate"].apply(extract_timestamp)
df_users["lastLogin"] = df_users["lastLogin"].apply(extract_timestamp)

# 3. Fill missing values in all columns with None
for col in df_users.columns:
    df_users[col] = df_users[col].apply(lambda x: None if pd.isna(x) or x == "" else x)

# 4. Remove duplicate `_id`
df_users.drop_duplicates(subset=["_id"], keep="first", inplace=True)

# Filter records where role equals "consumer"
df_users = df_users[df_users["role"] == "consumer"]

# Display processed users data overview
print("Processed users data overview:")
print(df_users.info())
print(df_users.head(10))

# Save the cleaned DataFrame as a CSV file
df_users.to_csv(os.path.join(cleaned_data_path, "users_cleaned.csv"), index=False, na_rep="NULL")

print("Data cleaning completed. The file users_cleaned.csv has been saved!")

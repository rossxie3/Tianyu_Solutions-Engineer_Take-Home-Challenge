import os
import pandas as pd
import ast

# Data paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(BASE_DIR, "data")
cleaned_data_path = os.path.join(data_path, "cleaned")

# Load the raw brands CSV file
df_brands = pd.read_csv(os.path.join(data_path, "brands_raw.csv"))

# 1. Parse `_id`
def extract_oid(oid_field):
    """
    Extract the ObjectId from a _id field.
    
    If the field is a string containing the pattern "{'$oid': ...}",
    the function extracts the actual oid value.
    """
    if isinstance(oid_field, str) and "{'$oid':" in oid_field:
        return oid_field.split(": '")[1].strip("'}")
    return oid_field

df_brands["_id"] = df_brands["_id"].apply(extract_oid)

# 2. Parse the 'cpg' field and extract the '$id'
def extract_cpg_id(cpg_field):
    """
    Extract the '$id' value from the cpg field.
    
    The function attempts to parse the string as a dictionary.
    If successful, it retrieves the nested '$oid' value; otherwise, it returns 'UNKNOWN'.
    """
    if isinstance(cpg_field, str):
        try:
            cpg_dict = ast.literal_eval(cpg_field)  
            return cpg_dict.get("$id", {}).get("$oid", "UNKNOWN")
        except:
            return "UNKNOWN"
    return "UNKNOWN"

df_brands["cpg_id"] = df_brands["cpg"].apply(extract_cpg_id)
# Drop the original cpg column as it's no longer needed
df_brands.drop(columns=["cpg"], inplace=True)

# 3. Remove 'test brand' data
# Filter out any rows where the brand name contains the substring "test" (case-insensitive)
df_brands = df_brands[~df_brands["name"].str.contains("test", case=False, na=False)]

# 4. Clean the 'topBrand' field
# Convert to string, then to lowercase, and finally map the values to boolean (True/False)
df_brands["topBrand"] = df_brands["topBrand"].astype(str).str.lower()
df_brands["topBrand"] = df_brands["topBrand"].map({"true": True, "false": False})
# Ensure that any missing values remain as None
df_brands["topBrand"] = df_brands["topBrand"].where(df_brands["topBrand"].notna(), None)

# 5. Fill missing 'brandCode'
# If 'brandCode' is missing, fill it with the value from 'barcode' (converted to string)
df_brands["brandCode"].fillna(df_brands["barcode"].astype(str), inplace=True)

# 6. Remove duplicates based on `_id`
# Keep only the first occurrence for each unique _id
df_brands.drop_duplicates(subset=["_id"], keep="first", inplace=True)

# Display processed brand data overview
print("Processed brand data overview:")
print(df_brands.info())
print(df_brands.head(10))  

# Save the cleaned DataFrame as a CSV file
df_brands.to_csv(os.path.join(cleaned_data_path, "brands_cleaned.csv"), index=False, na_rep="NULL")



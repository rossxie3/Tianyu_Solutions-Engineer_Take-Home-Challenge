import os
import pandas as pd
import json
import uuid
import ast

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "cleaned")
RECEIPTS_FILE = os.path.join(DATA_PATH, "receipts_cleaned.csv")
BRANDS_FILE = os.path.join(DATA_PATH, "brands_cleaned.csv")
OUTPUT_FILE = os.path.join(DATA_PATH, "receiptItems_cleaned.csv")

# Read data
df_receipts = pd.read_csv(RECEIPTS_FILE)
df_brands = pd.read_csv(BRANDS_FILE)

# Create mappings for 'barcode' and 'brandCode'
brand_barcode_map = df_brands.set_index("barcode")["name"].to_dict()
brand_code_map = df_brands.set_index("brandCode")["name"].to_dict()

def fix_json_string(json_str):
    """
    Fix the JSON structure:
      1. Solve nested quotation issues.
      2. Replace None/True/False values.
      3. Parse JSON.
    """
    if pd.isna(json_str) or json_str is None:
        return []

    if isinstance(json_str, str):
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(json_str)
            except (SyntaxError, ValueError):
                print(f"Warning: JSON parsing failed, attempting fix: {json_str[:200]}")
                json_str = json_str.replace("'", '"')
                json_str = json_str.replace('None', 'null').replace('True', 'true').replace('False', 'false')
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    return []
    return json_str if isinstance(json_str, list) else []

def extract_receipt_items(df):
    """
    Parse the rewardsReceiptItemList field from receipts and convert it into a structured format.
    
    For each receipt record:
      - The function attempts to parse the nested JSON in the 'rewardsReceiptItemList' field.
      - It creates a mapping between the barcode and the brand name using the brands data.
      - If the brand name is not found by barcode, it tries the brandCode; if still not found, it uses the barcode as the identifier.
      - The function builds a new record for each receipt item with fields such as _id, receiptId, brandCode, barcode, brandName, description, quantity, price, isBonus, and needsFetchReview.
    """
    receipt_items = []

    for _, row in df.iterrows():
        receipt_id = row["_id"]
        items_json = fix_json_string(row["rewardsReceiptItemList"])

        for item in items_json:
            brand_code = item.get("brandCode")
            barcode = item.get("barcode", item.get("userFlaggedBarcode", "UNKNOWN"))

            # First, attempt to find the brand name using the barcode.
            brand_name = brand_barcode_map.get(barcode)

            # If not found, try using brandCode.
            if brand_name is None and brand_code:
                brand_name = brand_code_map.get(brand_code)

            # If still not found, use the barcode as the brand identifier.
            if brand_name is None:
                brand_name = barcode

            receipt_items.append({
                "_id": str(uuid.uuid4()),
                "receiptId": receipt_id,
                "brandCode": brand_code if brand_code else "UNKNOWN",
                "barcode": barcode,
                "brandName": brand_name, 
                "description": item.get("description", item.get("userFlaggedDescription", "UNKNOWN")),
                "quantity": int(item.get("quantityPurchased", 1)),
                "price": float(item.get("finalPrice", item.get("userFlaggedPrice", 0.0))),
                "isBonus": bool(item.get("isBonus", False)),
                "needsFetchReview": bool(item.get("needsFetchReview", False))
            })

    return pd.DataFrame(receipt_items)

# Execute parsing
df_receipt_items = extract_receipt_items(df_receipts)

# Save to CSV
df_receipt_items.to_csv(OUTPUT_FILE, index=False)

print(f"receiptItems_cleaned.csv has been generated. Path: {OUTPUT_FILE}")

import sqlite3
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "fetch_data.db")

conn = sqlite3.connect(DB_PATH)



query = """


-- 1. Users table: Count records missing mandatory fields (_id, createdDate, or role)
SELECT 'Users' AS TableName,
       'Missing mandatory fields (_id, createdDate, or role)' AS Issue,
       COUNT(*) AS IssueCount
FROM users
WHERE _id IS NULL
   OR createdDate IS NULL
   OR role IS NULL

UNION ALL

-- 2. Receipts table: Count receipts where the userId does not exist in the Users table
SELECT 'Receipts' AS TableName,
       'User not found (userId missing in Users)' AS Issue,
       COUNT(*) AS IssueCount
FROM receipts
WHERE userId NOT IN (SELECT _id FROM users)

UNION ALL

-- 3. Receipts table: Count receipts with a negative totalSpent value
SELECT 'Receipts' AS TableName,
       'Negative totalSpent' AS Issue,
       COUNT(*) AS IssueCount
FROM receipts
WHERE totalSpent < 0

UNION ALL

-- 4. ReceiptItems table: Count receipt items with a receiptId that does not exist in Receipts
SELECT 'ReceiptItems' AS TableName,
       'Missing receipt (receiptId not in Receipts)' AS Issue,
       COUNT(*) AS IssueCount
FROM receiptItems
WHERE receiptId NOT IN (SELECT _id FROM receipts)

UNION ALL

-- 5. ReceiptItems table: Count receipt items with a brandCode that does not exist in Brands
SELECT 'ReceiptItems' AS TableName,
       'Missing brand (brandCode not in Brands)' AS Issue,
       COUNT(*) AS IssueCount
FROM receiptItems
WHERE brandCode NOT IN (SELECT brandCode FROM brands)

UNION ALL

-- 6. ReceiptItems table: Count receipt items with an invalid quantity (<= 0)
SELECT 'ReceiptItems' AS TableName,
       'Invalid quantity (<= 0)' AS Issue,
       COUNT(*) AS IssueCount
FROM receiptItems
WHERE quantity <= 0

UNION ALL

-- 7. ReceiptItems table: Count receipt items with a negative price
SELECT 'ReceiptItems' AS TableName,
       'Negative price (< 0)' AS Issue,
       COUNT(*) AS IssueCount
FROM receiptItems
WHERE price < 0;







"""

# Execute the query and fetch results
df = pd.read_sql(query, conn)
print(df)

# Close the connection
conn.close()

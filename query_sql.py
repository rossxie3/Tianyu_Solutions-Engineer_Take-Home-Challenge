import sqlite3
import pandas as pd
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "fetch_data.db")
queries = {
    "Q1: Top 5 brands by receipts scanned in the most recent month": """
        SELECT 
            b.name AS brand_name,
            COUNT(ri._id) AS scan_count
        FROM receiptItems AS ri
        JOIN receipts AS r ON ri.receiptId = r._id
        JOIN brands AS b ON ri.brandCode = b.brandCode
        WHERE r.rewardsReceiptStatus = 'FINISHED'
          AND r.dateScanned >= date((SELECT MAX(dateScanned) FROM receipts), '-1 month')
        GROUP BY b.name
        ORDER BY scan_count DESC
        LIMIT 5;
    """,
        
    "Q2: Comparison of top 5 brands by receipts scanned (recent month vs previous month)": """
        WITH periods AS (
            SELECT 
                b.name AS brand_name,
                COUNT(ri._id) AS scan_count,
                CASE 
                    WHEN r.dateScanned >= date((SELECT MAX(dateScanned) FROM receipts), '-1 month') 
                    THEN 'Recent Month'
                    ELSE 'Previous Month'
                END AS period
            FROM receiptItems AS ri
            JOIN receipts AS r ON ri.receiptId = r._id
            JOIN brands AS b ON ri.brandCode = b.brandCode
            WHERE r.rewardsReceiptStatus = 'FINISHED'
            AND r.dateScanned >= date((SELECT MAX(dateScanned) FROM receipts), '-2 month')
            GROUP BY b.name, period 
        )
        SELECT *
        FROM (
            SELECT *,
                RANK() OVER (PARTITION BY period ORDER BY scan_count DESC) AS rank
            FROM periods
        ) AS t
        WHERE rank <= 5
        ORDER BY period DESC, rank ASC;


    """,
    
    "Q3: Average spend comparison for 'FINISHED' and 'REJECTED' receipts": """
        SELECT 'FINISHED' AS status_type,
               COALESCE(AVG(totalSpent), 0) AS avg_spent
        FROM receipts 
        WHERE rewardsReceiptStatus = 'FINISHED'
        UNION ALL
        SELECT 'REJECTED' AS status_type,
               COALESCE(AVG(totalSpent), 0) AS avg_spent
        FROM receipts 
        WHERE rewardsReceiptStatus = 'REJECTED'
        UNION ALL
        SELECT 'RESULT' AS status_type,
               CASE 
                 WHEN COALESCE((SELECT AVG(totalSpent) FROM receipts WHERE rewardsReceiptStatus = 'FINISHED'), 0) > 
                      COALESCE((SELECT AVG(totalSpent) FROM receipts WHERE rewardsReceiptStatus = 'REJECTED'), 0)
                 THEN 'FINISHED has higher average spend'
                 ELSE 'REJECTED has higher average spend'
               END AS avg_spend_comparison;
    """,
    
    "Q4: Total number of items purchased comparison for 'FINISHED' and 'REJECTED' receipts": """
        SELECT 'FINISHED' AS status_type,
               COALESCE(SUM(purchasedItemCount), 0) AS total_items
        FROM receipts 
        WHERE rewardsReceiptStatus = 'FINISHED'
        UNION ALL
        SELECT 'REJECTED' AS status_type,
               COALESCE(SUM(purchasedItemCount), 0) AS total_items
        FROM receipts 
        WHERE rewardsReceiptStatus = 'REJECTED'
        UNION ALL
        SELECT 'RESULT' AS status_type,
               CASE 
                 WHEN COALESCE((SELECT SUM(purchasedItemCount) FROM receipts WHERE rewardsReceiptStatus = 'FINISHED'), 0) > 
                      COALESCE((SELECT SUM(purchasedItemCount) FROM receipts WHERE rewardsReceiptStatus = 'REJECTED'), 0)
                 THEN 'FINISHED has more items purchased'
                 ELSE 'REJECTED has more items purchased'
               END AS total_items_comparison;
    """,
    
    "Q5: Brand with the highest total spend among users created within the past 6 months": """
        SELECT 
            b.name AS brand_name, 
            SUM(r.totalSpent) AS total_spent
        FROM receipts AS r
        JOIN users AS u ON r.userId = u._id
        JOIN receiptItems AS ri ON r._id = ri.receiptId
        JOIN brands AS b ON ri.brandCode = b.brandCode
        WHERE r.rewardsReceiptStatus = 'FINISHED'
          AND strftime('%Y-%m-%d', u.createdDate) >= date((SELECT MAX(strftime('%Y-%m-%d', createdDate)) FROM users), '-6 month')
        GROUP BY b.name
        ORDER BY total_spent DESC
        LIMIT 5;
    """,
    
    "Q6: Brand with the highest transaction count among users created within the past 6 months": """
        SELECT 
            b.name AS brand_name, 
            COUNT(r._id) AS transaction_count
        FROM receipts AS r
        JOIN users AS u ON r.userId = u._id
        JOIN receiptItems AS ri ON r._id = ri.receiptId
        JOIN brands AS b ON ri.brandCode = b.brandCode
        WHERE r.rewardsReceiptStatus = 'FINISHED'
          AND strftime('%Y-%m-%d', u.createdDate) >= date((SELECT MAX(strftime('%Y-%m-%d', createdDate)) FROM users), '-6 month')
        GROUP BY b.name
        ORDER BY transaction_count DESC
        LIMIT 5;
    """
}

with sqlite3.connect(DB_PATH) as conn:
    for question, query in queries.items():
        print(f"\n{question}:\n")
        try:
            df = pd.read_sql(query, conn)
            print(df.to_string(index=False))
        except Exception as e:
            print(f"Error executing query: {e}")

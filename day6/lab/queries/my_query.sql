-- Customer transaction summary with intentional bugs
SELECT 
    ft.CUSTOMER_ID,
    ft.MERCHANT_ID,
    COUNT(*) as txn_count,
    SUM(ft.AMOUNT) as total_revenue,
    (SELECT COUNT(*) FROM FACT_TRANSACTIONS t2 WHERE t2.CUSTOMER_ID = ft.CUSTOMER_ID) as customer_txns
FROM FACT_TRANSACTIONS ft
LEFT JOIN DIM_MERCHANT dm ON ft.MERCHANT_ID = dm.MERCHANT_ID
WHERE ft.TRANSACTION_DATE >= '2024-01-01'
GROUP BY ft.CUSTOMER_ID, ft.MERCHANT_ID

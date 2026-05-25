-- Payment method failure analysis
SELECT 
    ft.PAYMENT_METHOD,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN ft.STATUS = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
    ROUND(100.0 * SUM(CASE WHEN ft.STATUS = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
FROM FACT_TRANSACTIONS ft
GROUP BY ft.PAYMENT_METHOD
ORDER BY failure_rate_pct DESC

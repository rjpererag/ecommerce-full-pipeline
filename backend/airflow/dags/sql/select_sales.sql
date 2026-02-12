SELECT
    COUNT(*) as total_orders
FROM fact_transactions
WHERE event_type = 'purchase' AND event_timestamp::date = '{{ ds }}'::date;

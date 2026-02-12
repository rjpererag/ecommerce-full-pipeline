DELETE FROM sales_performance
WHERE date_key = '{{ ds }}'::date;

INSERT INTO sales_performance  (
    date_key,
    total_orders,
    order_subtotal_day,
    order_tax_amount_day,
    order_discount_amount_day,
    order_total_amount_day,
    avg_ticket,
    avg_customer_ticket
)
WITH daily_purchases AS (
    SELECT
        customer_id,
        order_subtotal,
        order_tax_amount,
        order_discount_amount,
        order_total_amount
    FROM fact_transactions
    WHERE event_type = 'purchase'
      AND event_timestamp::date = '{{ ds }}'::date
),
customer_stats AS (
    SELECT
        customer_id,
        AVG(order_total_amount) as customer_daily_avg
    FROM daily_purchases
    GROUP BY customer_id
)
SELECT
    '{{ ds }}'::date as date_key,
    COUNT(*) as total_orders,
    SUM(COALESCE(order_subtotal, 0)) as order_subtotal_day,
    SUM(COALESCE(order_tax_amount, 0)) as order_tax_amount_day,
    SUM(COALESCE(order_discount_amount, 0)) as order_discount_amount_day,
    SUM(COALESCE(order_total_amount, 0)) as order_total_amount_day,
    AVG(COALESCE(order_total_amount, 0)) as avg_ticket,
    COALESCE((SELECT AVG(customer_daily_avg) FROM customer_stats), 0) as avg_customer_ticket
FROM daily_purchases;

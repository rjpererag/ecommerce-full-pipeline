DELETE FROM item_category_performance WHERE date_key = '{{ ds }}';

INSERT INTO item_category_performance (
    date_key,
    category,
    total_quantity,
    avg_unit_price,
    avg_quantity,
    avg_discount,
    avg_cost
)
SELECT
    event_timestamp::DATE AS date_key,
    category,
    SUM(quantity) AS total_quantity,
    AVG(unit_price) AS avg_unit_price,
    AVG(quantity) AS avg_quantity,
    AVG(discount) AS avg_discount,
    AVG("cost") AS avg_cost
FROM fact_transaction_items
WHERE event_timestamp::DATE = '{{ ds }}'
GROUP BY 1, 2;
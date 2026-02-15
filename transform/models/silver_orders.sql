-- Silver: cleaned and deduplicated orders, only completed
MODEL (
    name demo.silver_orders,
    kind FULL
);

SELECT DISTINCT
    order_id,
    customer_id,
    product,
    quantity,
    unit_price,
    quantity * unit_price AS total_price,
    CAST(order_date AS DATE) AS order_date
FROM demo.bronze_orders
WHERE status = 'completed';

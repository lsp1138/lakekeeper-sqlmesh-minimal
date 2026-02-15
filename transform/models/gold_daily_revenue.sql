-- Gold: aggregated daily revenue, business-ready
MODEL (
    name demo.gold_daily_revenue,
    kind FULL
);

SELECT
    order_date,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_price) AS revenue,
    ROUND(SUM(total_price) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM demo.silver_orders
GROUP BY order_date
ORDER BY order_date;

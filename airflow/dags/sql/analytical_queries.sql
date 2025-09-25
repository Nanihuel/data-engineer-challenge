-- Top 10 products by revenue in USD
SELECT p.product_id, p.product_name, p.category,
       SUM(f.quantity) AS units_sold,
       ROUND(SUM(f.gross_amount_usd), 2) AS revenue_usd
FROM dwh.fact_sales f
JOIN dwh.dim_product p USING (product_id)
GROUP BY 1,2,3
ORDER BY revenue_usd DESC
LIMIT 10;

-- Orders and revenue by hour of day (0-23)
SELECT hour_of_day,
       COUNT(DISTINCT order_id) AS orders_cnt,
       ROUND(SUM(gross_amount_usd), 2) AS revenue_usd
FROM dwh.fact_sales
GROUP BY hour_of_day
ORDER BY revenue_usd DESC;

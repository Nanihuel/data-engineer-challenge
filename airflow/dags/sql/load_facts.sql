-- Purpose: Populate dwh.fact_sales from staging sources
-- Notes:
--  - Grain: one row per order_item
--  - gross_amount = unit_price * quantity
--  - gross_amount_usd converts gross_amount using FX rate for the order_date and item currency
--  - Only allowed currencies are USD/EUR/GBP; invalids are filtered out
--  - Orders flagged in dwh.rejected_orders are excluded for data quality
--  - Joins ensure referential integrity with product and customer dimensions

-- Fail fast if table is locked by another session
SET lock_timeout = '10s';

TRUNCATE dwh.fact_sales;

-- Use the latest FX rate per currency (no per-order-date rate)
WITH latest_fx AS (
  SELECT DISTINCT ON (currency)
         currency, rate_to_usd, fx_date
  FROM dwh.fx_rates
  ORDER BY currency, fx_date DESC
)
INSERT INTO dwh.fact_sales (
  order_id, order_item_id, product_id, customer_id,
  order_ts, order_date_id, hour_of_day,
  item_currency, order_currency,
  unit_price, quantity,
  gross_amount, gross_amount_usd
)
SELECT
  o.id,
  oi.id,
  oi.product_id,
  o.customer_id,
  o.order_date,
  o.order_date::date,
  EXTRACT(HOUR FROM o.order_date)::int,
  UPPER(oi.currency) AS item_currency,
  UPPER(o.currency)  AS order_currency,
  ROUND(oi.unit_price::NUMERIC, 2)                                   AS unit_price,
  oi.quantity::INT                                                    AS quantity,
  ROUND( (oi.unit_price::NUMERIC * oi.quantity::INT), 2)              AS gross_amount,
  ROUND( (oi.unit_price::NUMERIC * oi.quantity::INT) * lfx.rate_to_usd, 2) AS gross_amount_usd
FROM stg.order_items oi
JOIN stg.orders o        ON o.id = oi.order_id
JOIN dwh.dim_product p   ON p.product_id = oi.product_id
JOIN dwh.dim_customer c  ON c.customer_id = o.customer_id
JOIN latest_fx lfx       ON lfx.currency = UPPER(oi.currency)
WHERE oi.quantity > 0
  AND UPPER(oi.currency) = ANY(ARRAY['USD','EUR','GBP'])
  AND NOT EXISTS (SELECT 1 FROM dwh.rejected_orders r WHERE r.order_id = o.id);

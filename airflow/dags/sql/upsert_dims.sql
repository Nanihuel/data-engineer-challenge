-- Purpose: Build/update warehouse dimensions from staging
-- Strategy: Insert new keys, update existing attributes using ON CONFLICT upsert semantics
-- dim_date: generate a contiguous date range based on order dates
WITH bounds AS (
  SELECT MIN(order_date)::date AS min_d, MAX(order_date)::date AS max_d FROM stg.orders
), series AS (
  SELECT generate_series(min_d, max_d, interval '1 day')::date AS d FROM bounds
)
INSERT INTO dwh.dim_date (date_id, year, quarter, month, day, dow, is_weekend)
SELECT d,
       EXTRACT(YEAR FROM d)::int,
       EXTRACT(QUARTER FROM d)::int,
       EXTRACT(MONTH FROM d)::int,
       EXTRACT(DAY FROM d)::int,
       EXTRACT(DOW FROM d)::int,
       (EXTRACT(DOW FROM d)::int IN (0,6))
FROM series
ON CONFLICT (date_id) DO NOTHING;  -- date dimension is immutable per date_id

-- dim_customer: upsert customer attributes from staging
INSERT INTO dwh.dim_customer (customer_id, name, email, registration_date, country)
SELECT id, name, email, registration_date, country
FROM stg.customers
ON CONFLICT (customer_id) DO UPDATE
  SET name=EXCLUDED.name,
      email=EXCLUDED.email,
      registration_date=EXCLUDED.registration_date,
      country=EXCLUDED.country;

-- dim_product: upsert product attributes from product catalog
INSERT INTO dwh.dim_product (product_id, product_name, category, base_price, base_currency)
SELECT id, name, category, base_price, currency
FROM stg.product_descriptions
ON CONFLICT (product_id) DO UPDATE
  SET product_name=EXCLUDED.product_name,
      category=EXCLUDED.category,
      base_price=EXCLUDED.base_price,
      base_currency=EXCLUDED.base_currency;

-- Data Warehouse schema
CREATE SCHEMA IF NOT EXISTS dwh;

-- Date dimension (hour is stored in the fact)
CREATE TABLE IF NOT EXISTS dwh.dim_date (
  date_id     DATE PRIMARY KEY,
  year        INT,
  quarter     INT,
  month       INT,
  day         INT,
  dow         INT,         -- day of week (0=sunday on Postgres)
  is_weekend  BOOLEAN
);

-- Product dimension (from DB2)
CREATE TABLE IF NOT EXISTS dwh.dim_product (
  product_id    INT PRIMARY KEY,
  product_name  VARCHAR(200),
  category      VARCHAR(100),
  base_price    NUMERIC(10,2),
  base_currency VARCHAR(3)
);

-- Customer dimension (enriched with available attributes)
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
  customer_id        INT PRIMARY KEY,
  name               VARCHAR(100),
  email              VARCHAR(150),
  registration_date  TIMESTAMP,
  country            VARCHAR(50)
);

-- Exchange rates (FX) to USD
CREATE TABLE IF NOT EXISTS dwh.fx_rates (
  fx_date     DATE,
  currency    VARCHAR(3),
  rate_to_usd NUMERIC(18,6) NOT NULL,
  PRIMARY KEY (fx_date, currency)
);

-- Data quality tables
CREATE TABLE IF NOT EXISTS dwh.rejected_orders (
  order_id    INT PRIMARY KEY,
  reason      TEXT NOT NULL,
  detected_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dq_issues (
  issue_id    BIGSERIAL PRIMARY KEY,
  entity      TEXT NOT NULL,     -- 'order', 'order_item', ...
  entity_id   INT NOT NULL,
  issue_code  TEXT NOT NULL,     -- 'NO_ITEMS', 'INVALID_*', 'CURRENCY_MISMATCH', ...
  detail      TEXT,
  detected_at TIMESTAMP DEFAULT NOW()
);

-- Sales fact (grain: order_item; item currency)
CREATE TABLE IF NOT EXISTS dwh.fact_sales (
  order_id         INT,
  order_item_id    INT,
  product_id       INT,
  customer_id      INT,
  order_ts         TIMESTAMP,
  order_date_id    DATE,
  hour_of_day      INT,
  item_currency    VARCHAR(3),           -- item currency (used for FX conversion)
  order_currency   VARCHAR(3),           -- currency declared on the order (used for DQ/analytics)
  unit_price       NUMERIC(18,2) CHECK (unit_price >= 0),
  quantity         INTEGER CHECK (quantity > 0),
  gross_amount     NUMERIC(18,2),        -- unit_price * quantity
  gross_amount_usd NUMERIC(18,2),        -- gross_amount * rate_to_usd

  PRIMARY KEY (order_id, order_item_id),
  FOREIGN KEY (order_date_id) REFERENCES dwh.dim_date(date_id),
  FOREIGN KEY (product_id)    REFERENCES dwh.dim_product(product_id),
  FOREIGN KEY (customer_id)   REFERENCES dwh.dim_customer(customer_id)
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS ix_fact_sales_product  ON dwh.fact_sales(product_id);
CREATE INDEX IF NOT EXISTS ix_fact_sales_date     ON dwh.fact_sales(order_date_id);
CREATE INDEX IF NOT EXISTS ix_fact_sales_hour     ON dwh.fact_sales(hour_of_day);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customer ON dwh.fact_sales(customer_id);

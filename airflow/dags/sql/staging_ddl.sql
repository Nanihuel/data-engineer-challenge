-- Staging schema for raw ingestions
-- Purpose: Temporary landing zone for upstream source data (orders, items, customers, products)
-- Notes:
--  - Minimal constraints to mirror source systems
--  - Data quality validation happens downstream before loading the warehouse
--  - Types match source DBs for straightforward loads

CREATE SCHEMA IF NOT EXISTS stg;

-- Orders header table: one row per order placed by a customer
CREATE TABLE IF NOT EXISTS stg.orders (
  id           INT PRIMARY KEY,
  customer_id  INT NOT NULL,
  order_date   TIMESTAMP NOT NULL,
  total_amount NUMERIC(10,2) NOT NULL,
  currency     VARCHAR(3) NOT NULL,
  status       VARCHAR(20) NOT NULL
);

-- Order line items: details for each product included in an order
CREATE TABLE IF NOT EXISTS stg.order_items (
  id          INT PRIMARY KEY,
  order_id    INT NOT NULL,
  product_id  INT NOT NULL,
  quantity    INT NOT NULL,
  unit_price  NUMERIC(10,2) NOT NULL,
  currency    VARCHAR(3) NOT NULL
);

-- Customers master data as provided by the source system
CREATE TABLE IF NOT EXISTS stg.customers (
  id                 INT PRIMARY KEY,
  name               VARCHAR(100) NOT NULL,
  email              VARCHAR(150) NOT NULL,
  registration_date  TIMESTAMP NOT NULL,
  country            VARCHAR(50) NOT NULL
);

-- Product catalog/descriptions as provided by the source system
CREATE TABLE IF NOT EXISTS stg.product_descriptions (
  id          INT PRIMARY KEY,
  name        VARCHAR(200) NOT NULL,
  category    VARCHAR(100) NOT NULL,
  description TEXT,
  base_price  NUMERIC(10,2) NOT NULL,
  currency    VARCHAR(3) NOT NULL
);

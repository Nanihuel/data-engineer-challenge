# Data Engineer Challenge – eCommerce Analytics

End-to-end solution for the Warehouse and Pipeline Design for eCommerce Analytics challenge.

## 🎯 Goals
- Build a dimensional Data Warehouse that answers:
  - Top products by units and revenue (USD).
  - Best hour of day to run promotions based on historical patterns.
- Deliver a functional Airflow DAG that loads staging, runs data quality checks, upserts dimensions, fetches FX rates, and loads the fact.
- Provide documentation of design decisions and how to run it locally.

---

## 🧱 Architecture (ELT)

Sources on separate Postgres DBs → staged raw copies → DQ → dimensions → FX snapshot → fact

```
DB1 (orders, order_items, customers)     DB2 (product_descriptions)
                 \                         /
                  \                       /
                   v                     v
                    [ stg.* (1:1) ] → DQ → Dimensions → FX (daily snapshot) → Fact (order_item)
```

Key choices:
- Grain: `fact_sales` at order_item level.
- Currency: use the item currency from `stg.order_items`; convert to USD via FX.
- FX: store one row per currency per day (daily snapshot). The fact uses the latest available rate per currency.
- Data Quality: explicit rules stored in `dwh.dq_issues` and `dwh.rejected_orders`.

---

## 📁 Repository Layout

```
data-engineer-challenge/
├─ docker-compose.yml
├─ requirements.txt
├─ README.md
├─ design_process.md  ← this document
├─ database/
│  ├─ init_db1.sql
│  ├─ init_db2.sql
│  └─ init_warehouse.sql
└─ airflow/
   ├─ dags/
   │  ├─ ecommerce_etl.py           # Airflow DAG (staging → DQ → dims → FX → fact)
   │  └─ sql/
   │     ├─ create_dwh.sql          # DWH DDL (dimensions, fact, dq, fx)
   │     ├─ staging_ddl.sql         # staging DDL (1:1 with sources)
   │     ├─ dq_checks.sql           # data quality rules
   │     ├─ upsert_dims.sql         # populate dimensions
   │     └─ load_facts.sql          # load fact using latest FX
   └─ logs/                         # Airflow runtime logs
```

---

## ▶️ How to Run Locally

### 1) Spin up containers
Windows PowerShell from the repo root:

```powershell
docker compose up -d
```

### 2) Initialize the Warehouse schemas (run once)
Windows PowerShell:

```powershell
Get-Content -Raw .\airflow\dags\sql\create_dwh.sql | docker exec -i data_warehouse psql -U postgres -d data_warehouse
Get-Content -Raw .\airflow\dags\sql\staging_ddl.sql | docker exec -i data_warehouse psql -U postgres -d data_warehouse
```

Linux/macOS alternative:

```bash
docker exec -i data_warehouse psql -U postgres -d data_warehouse < airflow/dags/sql/create_dwh.sql
docker exec -i data_warehouse psql -U postgres -d data_warehouse < airflow/dags/sql/staging_ddl.sql
```

### 3) Configure Airflow Connections (UI → Admin → Connections)
- src_db1 → Postgres
  - Host: postgres_db1, Port: 5432, DB: ecommerce_orders, User/Pass: postgres/postgres
- src_db2 → Postgres
  - Host: postgres_db2, Port: 5432, DB: ecommerce_products, User/Pass: postgres/postgres
- dwh_db → Postgres
  - Host: postgres_warehouse, Port: 5432, DB: data_warehouse, User/Pass: postgres/postgres

### 4) Trigger the DAG
Open Airflow at http://localhost:8080, enable and trigger `ecommerce_etl`.

---

## ✅ Validation Checklist
Run these in the `data_warehouse` database:

```sql
-- Staging and dimensions exist
SELECT COUNT(*) FROM stg.orders;
SELECT COUNT(*) FROM stg.order_items;
SELECT COUNT(*) FROM stg.customers;
SELECT COUNT(*) FROM stg.product_descriptions;

SELECT COUNT(*) FROM dwh.dim_product;
SELECT COUNT(*) FROM dwh.dim_customer;
SELECT COUNT(*) FROM dwh.dim_date;

-- Fact and DQ sanity
SELECT COUNT(*) FROM dwh.fact_sales;                                  
SELECT COUNT(*) FROM dwh.fact_sales WHERE gross_amount_usd IS NULL;    -- should be 0

SELECT issue_code, COUNT(*) 
FROM dwh.dq_issues 
GROUP BY 1 ORDER BY 2 DESC;

-- Expected vs loaded (requires product exists)
WITH eligible AS (
  SELECT oi.id
  FROM stg.order_items oi
  JOIN stg.orders o ON o.id = oi.order_id
  JOIN stg.product_descriptions p ON p.id = oi.product_id
  WHERE oi.quantity > 0
    AND UPPER(oi.currency) IN ('USD','EUR','GBP')
    AND NOT EXISTS (SELECT 1 FROM dwh.rejected_orders r WHERE r.order_id = o.id)
)
SELECT (SELECT COUNT(*) FROM eligible) AS expected_refined,
       (SELECT COUNT(*) FROM dwh.fact_sales) AS in_fact;
```

FX note: `dwh.fx_rates` stores one row per currency per day. Multiple DAG runs on the same day will upsert (not duplicate). The fact uses the latest FX per currency.

---

## 📊 Business Queries
Top products (USD):

```sql
SELECT p.product_id, p.product_name, p.category,
       SUM(f.quantity) AS units_sold,
       ROUND(SUM(f.gross_amount_usd), 2) AS revenue_usd
FROM dwh.fact_sales f
JOIN dwh.dim_product p USING (product_id)
GROUP BY 1,2,3
ORDER BY revenue_usd DESC
LIMIT 10;
```

Best hour of day (USD):

```sql
SELECT hour_of_day,
       COUNT(DISTINCT order_id) AS orders_cnt,
       ROUND(SUM(gross_amount_usd), 2) AS revenue_usd
FROM dwh.fact_sales
GROUP BY hour_of_day
ORDER BY revenue_usd DESC;
```

Optional: for a heatmap, also join `dwh.dim_date` to use `dow`/`is_weekend`.

---

## 🧪 Data Quality Rules
Implemented in `airflow/dags/sql/dq_checks.sql`:

- `NO_ITEMS` — orders without items (excluded from fact).
- `INVALID_ORDER_CURRENCY` and `INVALID_ITEM_CURRENCY` — only USD/EUR/GBP accepted in this MVP.
- `CURRENCY_MISMATCH` — order and item currencies differ.
- Catalog coverage:
  - `MISSING_PRODUCT` — all order_items whose `product_id` does not exist in the catalog (overall catalog health).
  - `MISSING_PRODUCT_ELIGIBLE` — the subset that would otherwise qualify for the fact (the actionable blockers).

The same order_item can have multiple issues; that’s intentional to capture different DQ dimensions.

---

## 🧰 Modeling & Types
- `fact_sales` grain: order_item.
- Types:
  - `quantity`: INTEGER
  - Monetary: NUMERIC(18,2) for `unit_price`, `gross_amount`, `gross_amount_usd`
  - FX: NUMERIC(18,6) for `rate_to_usd`
- Useful indexes in `fact_sales`: `(product_id)`, `(order_date_id)`, `(hour_of_day)`, `(customer_id)`.
- `dim_date` includes `dow` and `is_weekend` (uses `EXTRACT(DOW ...)`, Sunday = 0 in Postgres).

---

## ⚙️ Performance & Idempotency
- Staging uses TRUNCATE then bulk load (idempotent).
- FX task upserts per `(fx_date, currency)` — one snapshot per day, per currency.
- `load_facts.sql` precomputes the latest FX per currency via a CTE with `DISTINCT ON (currency)` and joins once.
- To fail fast on lock contention, `load_facts.sql` sets `SET lock_timeout = '10s';` before `TRUNCATE`.
- Optional index for FX join performance as data grows:
  - `CREATE INDEX IF NOT EXISTS ix_fx_currency_date ON dwh.fx_rates(currency, fx_date DESC);`

---

## 📌 Assumptions
- FX endpoint is `latest` (not historical). The fact uses the latest snapshot available on each run.
- Supported currencies for the MVP: USD, EUR, GBP. Others are recorded as DQ issues and excluded from the fact.
- `orders.status` is not used as a load filter; quality is driven by presence of items and DQ checks.

---

## 🧯 Troubleshooting
- relation `stg.*` does not exist → run `staging_ddl.sql` once against `data_warehouse`.
- `gross_amount_usd` has NULLs → missing FX for a used currency; ensure the FX task ran and rows exist in `dwh.fx_rates`.
- DAG doesn’t show in UI → restart the scheduler container.
- Slow `load_facts` → ensure the latest-FX CTE is used and consider the FX index above.
- Task stuck on TRUNCATE → a session is holding locks; either close it or terminate the blocker; `lock_timeout` helps fail fast.

---

## 📦 Deliverables Mapping
- SQL DWH DDL → `airflow/dags/sql/create_dwh.sql`
- Airflow DAG + ETL → `airflow/dags/ecommerce_etl.py` (+ SQLs in `airflow/dags/sql/`)
- Design rationale → `design_process.md`
- Dashboard mockup → `Mockup.png`
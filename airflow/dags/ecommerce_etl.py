"""
Ecommerce ETL DAG

Flow:
1) Load staging tables from source Postgres DBs (bulk inserts into stg.*)
2) Run data quality checks (external SQL) generating rejects/issues
3) Upsert dimensions (date, customer, product) from staging (external SQL)
4) Upsert FX rates via public API (fallback constants if API unavailable)
5) Load sales fact table using item currency and FX rates (external SQL)

Notes:
- Template search path points to /opt/airflow/dags/sql for external SQL files
- Only USD/EUR/GBP are considered valid item currencies
"""

from __future__ import annotations
from datetime import datetime
import logging
from typing import List, Tuple
from collections import defaultdict
from datetime import date
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#try:
#except Exception:
#    requests = None  # The DAG continues with a fallback if requests is not available

DAG_ID = "ecommerce_etl"
VALID_ITEM_CURRENCIES = {"USD", "EUR", "GBP"}  # Non-listed currencies are excluded via DQ

default_args = {"owner": "nahuel", "retries": 0}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["challenge", "ecommerce"],
    template_searchpath=["/opt/airflow/dags/sql"],  # reads external .sql files from this folder
) as dag:

    # 1) STAGING: copy sources into stg.* (TRUNCATE + bulk insert)
    def load_staging():
        """Extract data from source DBs and bulk load into staging tables (stg.*)."""
        src1 = PostgresHook("src_db1")  # orders, order_items, customers
        src2 = PostgresHook("src_db2")  # product_descriptions
        dwh = PostgresHook("dwh_db")

        def fetch_all(hook: PostgresHook, sql: str):
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    return cur.fetchall()

        orders = fetch_all(src1, """
            SELECT id, customer_id, order_date, total_amount, currency, status
            FROM public.orders;
        """)
        order_items = fetch_all(src1, """
            SELECT id, order_id, product_id, quantity, unit_price, currency
            FROM public.order_items;
        """)
        customers = fetch_all(src1, """
            SELECT id, name, email, registration_date, country
            FROM public.customers;
        """)
        products = fetch_all(src2, """
            SELECT id, name, category, description, base_price, currency
            FROM public.product_descriptions;
        """)

        with dwh.get_conn() as conn:
            with conn.cursor() as cur:
                # STG tables already exist (created outside the DAG)
                cur.execute("TRUNCATE stg.orders;")
                cur.execute("TRUNCATE stg.order_items;")
                cur.execute("TRUNCATE stg.customers;")
                cur.execute("TRUNCATE stg.product_descriptions;")

                execute_values(cur,
                    "INSERT INTO stg.orders (id, customer_id, order_date, total_amount, currency, status) VALUES %s",
                    orders or [])
                execute_values(cur,
                    "INSERT INTO stg.order_items (id, order_id, product_id, quantity, unit_price, currency) VALUES %s",
                    order_items or [])
                execute_values(cur,
                    "INSERT INTO stg.customers (id, name, email, registration_date, country) VALUES %s",
                    customers or [])
                execute_values(cur,
                    "INSERT INTO stg.product_descriptions (id, name, category, description, base_price, currency) VALUES %s",
                    products or [])
            conn.commit()

    t_load_staging = PythonOperator(task_id="load_staging", python_callable=load_staging)

    # 2) DQ (rejects/issues) — external SQL
    dq_checks = PostgresOperator(
        task_id="dq_checks",
        postgres_conn_id="dwh_db",
        sql="dq_checks.sql",
    )

    # 3) Dimensions (date/customer/product) — external SQL
    upsert_dims = PostgresOperator(
        task_id="upsert_dims",
        postgres_conn_id="dwh_db",
        sql="upsert_dims.sql",
    )

    # 4) FX via public API (latest/{base})
    def upsert_fx_rates():
        
        """Fetch FX rates from public API and upsert into dwh.fx_rates table."""

        dwh = PostgresHook("dwh_db")

        # Valid currencies actually present in STG (in case any is missing someday)
        with dwh.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT UPPER(oi.currency)
                    FROM stg.order_items oi
                    WHERE UPPER(oi.currency) = ANY(%s)
                """, (list(VALID_ITEM_CURRENCIES),))
                currencies = [row[0] for row in cur.fetchall()]

        if not currencies:
            logging.info("No currencies in staging; skipping FX.")
            return

        rows = []
        fallback = {"USD": 1.0, "EUR": 1.10, "GBP": 1.25}
        as_of = date.today().isoformat()  # per-run snapshot

        for ccy in currencies:
            if ccy == "USD":
                rate_to_usd = 1.0
            else:
                try:
                    if not requests:
                        raise RuntimeError("requests not available")
                    resp = requests.get(f"https://api.exchangerate-api.com/v4/latest/{ccy}", timeout=10)
                    resp.raise_for_status()
                    data = resp.json()
                    rate_to_usd = float(data["rates"]["USD"])
                except Exception as e:
                    logging.warning("FX API error for %s: %s; using fallback", ccy, e)
                    rate_to_usd = fallback.get(ccy, 0.0)
            rows.append((as_of, ccy, rate_to_usd))

        with dwh.get_conn() as conn:
            with conn.cursor() as cur:
                values = ",".join(cur.mogrify("(%s,%s,%s)", r).decode("utf-8") for r in rows)
                cur.execute(f"""
                    INSERT INTO dwh.fx_rates (fx_date, currency, rate_to_usd)
                    VALUES {values}
                    ON CONFLICT (fx_date, currency)
                    DO UPDATE SET rate_to_usd = EXCLUDED.rate_to_usd
                """)
            conn.commit()
    t_fx = PythonOperator(task_id="upsert_fx_rates", python_callable=upsert_fx_rates)

    # 5) FACT — external SQL (uses item_currency and previously loaded FX)
    load_facts = PostgresOperator(
        task_id="load_facts",
        postgres_conn_id="dwh_db",
        sql="load_facts.sql",
    )

    # Orchestration
    t_load_staging >> dq_checks >> upsert_dims >> t_fx >> load_facts

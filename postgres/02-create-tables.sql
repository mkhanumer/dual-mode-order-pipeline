-- Destination tables for the batch pipeline (etl/load.py + Airflow)
-- and the streaming pipeline (spark_streaming_job.py).

CREATE TABLE IF NOT EXISTS orders_clean (
    order_id          BIGINT PRIMARY KEY,
    customer_name     TEXT,
    product_name      TEXT,
    category          TEXT,
    unit_price        NUMERIC(10,2),
    quantity          INTEGER,
    order_date        DATE,
    country           TEXT,
    total_amount      NUMERIC(10,2),
    order_year_month  TEXT
);

CREATE TABLE IF NOT EXISTS live_category_revenue (
    window_start   TIMESTAMP,
    window_end     TIMESTAMP,
    category       TEXT,
    total_revenue  NUMERIC(12,2),
    order_count    BIGINT
);

CREATE TABLE IF NOT EXISTS daily_orders_summary (
    summary_date     DATE,
    category         TEXT,
    total_orders     INTEGER,
    total_revenue    NUMERIC(12,2),
    generated_at     TIMESTAMP DEFAULT NOW()
);

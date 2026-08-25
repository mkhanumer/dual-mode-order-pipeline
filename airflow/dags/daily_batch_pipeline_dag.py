import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# etl/ folder is mounted at /opt/airflow/etl inside docker
sys.path.append("/opt/airflow/etl")

RAW_CSV_PATH = "/opt/airflow/data/raw_orders_messy.csv"


def extract_task_fn(ti):
    from extract import extract_from_csv

    df = extract_from_csv(RAW_CSV_PATH)

    # DataFrames are too big for XCom, so save to a temp file and pass the path
    path = "/tmp/raw_orders.parquet"
    df.to_parquet(path)
    ti.xcom_push(key="raw_path", value=path)


def clean_task_fn(ti):
    import pandas as pd
    from clean import clean_orders

    raw_path = ti.xcom_pull(key="raw_path", task_ids="extract_data")
    raw_df = pd.read_parquet(raw_path)
    clean_df = clean_orders(raw_df)

    path = "/tmp/clean_orders.parquet"
    clean_df.to_parquet(path)
    ti.xcom_push(key="clean_path", value=path)


def quality_check_task_fn(ti):
    """Fail loudly if cleaned data is broken, so bad rows never reach Postgres."""
    import pandas as pd

    clean_path = ti.xcom_pull(key="clean_path", task_ids="clean_data")
    df = pd.read_parquet(clean_path)

    if len(df) == 0:
        raise ValueError("Cleaned dataset is empty")
    if df["unit_price"].min() <= 0:
        raise ValueError("Found non-positive price")
    if not df["order_id"].is_unique:
        raise ValueError("Duplicate order_id found")
    if df["order_date"].isna().sum() > 0:
        raise ValueError("Null order_date found")

    print(f"[quality_check] Passed. {len(df)} rows are safe to load.")


def load_task_fn(ti):
    import pandas as pd
    from load import get_postgres_engine, load_to_postgres

    clean_path = ti.xcom_pull(key="clean_path", task_ids="clean_data")
    df = pd.read_parquet(clean_path)

    engine = get_postgres_engine(host="postgres")
    load_to_postgres(df, "orders_clean", engine)


def summarize_task_fn(ti):
    """Group today's orders by category and store daily totals."""
    import pandas as pd
    from load import get_postgres_engine

    clean_path = ti.xcom_pull(key="clean_path", task_ids="clean_data")
    df = pd.read_parquet(clean_path)

    summary = (
        df.groupby("category")
        .agg(total_orders=("order_id", "count"), total_revenue=("total_amount", "sum"))
        .reset_index()
    )
    summary["summary_date"] = datetime.utcnow().date()

    engine = get_postgres_engine(host="postgres")
    summary.to_sql("daily_orders_summary", engine, if_exists="append", index=False)
    print(f"[summarize] Wrote {len(summary)} category summary rows")


default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="daily_batch_pipeline",
    description="Daily order pipeline: extract, clean, quality check, load, summarize",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    extract_data = PythonOperator(task_id="extract_data", python_callable=extract_task_fn)
    clean_data = PythonOperator(task_id="clean_data", python_callable=clean_task_fn)
    quality_check = PythonOperator(task_id="quality_check", python_callable=quality_check_task_fn)
    load_data = PythonOperator(task_id="load_data", python_callable=load_task_fn)
    summarize = PythonOperator(task_id="summarize_data", python_callable=summarize_task_fn)

    # if any step fails here, everything after it never runs
    extract_data >> clean_data >> quality_check >> load_data >> summarize

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# etl/ is mounted at /opt/airflow/etl in docker (see docker-compose.yml volumes)
sys.path.append("/opt/airflow/etl")

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def extract_task_fn(ti, **kwargs):
    from extract import extract_from_csv
    df = extract_from_csv("/opt/airflow/data/raw_orders_messy.csv")
    path = "/tmp/raw_orders.parquet"
    df.to_parquet(path)
    ti.xcom_push(key="raw_path", value=path)


def clean_task_fn(ti, **kwargs):
    import pandas as pd
    from clean import clean_orders

    raw_path = ti.xcom_pull(key="raw_path", task_ids="extract_data")
    raw_df = pd.read_parquet(raw_path)
    clean_df = clean_orders(raw_df)

    path = "/tmp/clean_orders.parquet"
    clean_df.to_parquet(path)
    ti.xcom_push(key="clean_path", value=path)


def quality_check_task_fn(ti, **kwargs):
    import pandas as pd

    clean_path = ti.xcom_pull(key="clean_path", task_ids="clean_data")
    df = pd.read_parquet(clean_path)

    assert len(df) > 0, "Quality check failed: cleaned dataset is empty"
    assert df["unit_price"].min() > 0, "Quality check failed: found non-positive price"
    assert df["order_id"].is_unique, "Quality check failed: duplicate order_id found"
    assert df["order_date"].isna().sum() == 0, "Quality check failed: null order_date found"

    print(f"[quality_check] Passed. {len(df)} rows are safe to load.")


def load_task_fn(ti, **kwargs):
    import pandas as pd
    from load import get_postgres_engine, load_to_postgres

    clean_path = ti.xcom_pull(key="clean_path", task_ids="clean_data")
    df = pd.read_parquet(clean_path)

    engine = get_postgres_engine(host="postgres")
    load_to_postgres(df, table_name="orders_clean", engine=engine, if_exists="append")


def summarize_task_fn(ti, **kwargs):
    import pandas as pd
    from load import get_postgres_engine

    engine = get_postgres_engine(host="postgres")
    clean_path = ti.xcom_pull(key="clean_path", task_ids="clean_data")
    df = pd.read_parquet(clean_path)

    summary = (
        df.groupby("category")
        .agg(total_orders=("order_id", "count"), total_revenue=("total_amount", "sum"))
        .reset_index()
    )
    summary["summary_date"] = datetime.utcnow().date()

    summary.to_sql("daily_orders_summary", engine, if_exists="append", index=False)
    print(f"[summarize] Wrote {len(summary)} category summary rows")


with DAG(
    dag_id="daily_batch_pipeline",
    description="Extract, clean, quality-check, load and summarize daily order data",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "orders", "warehouse"],
) as dag:

    extract_data = PythonOperator(task_id="extract_data", python_callable=extract_task_fn)
    clean_data = PythonOperator(task_id="clean_data", python_callable=clean_task_fn)
    quality_check = PythonOperator(task_id="quality_check", python_callable=quality_check_task_fn)
    load_data = PythonOperator(task_id="load_data", python_callable=load_task_fn)
    summarize = PythonOperator(task_id="summarize_data", python_callable=summarize_task_fn)

    extract_data >> clean_data >> quality_check >> load_data >> summarize

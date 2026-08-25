import os
import pandas as pd
from sqlalchemy import create_engine


def get_postgres_engine(
    host: str = "postgres",
    port: int = 5432,
    db: str = "warehouse",
    user: str = None,
    password: str = None,
):
    if user is None:
        user = os.getenv("POSTGRES_USER", "de_user")
    if password is None:
        password = os.getenv("POSTGRES_PASSWORD", "de_password")
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str)


def load_to_postgres(df: pd.DataFrame, table_name: str, engine, if_exists: str = "append"):
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"[load] Wrote {len(df)} rows into table '{table_name}' (mode={if_exists})")


if __name__ == "__main__":
    from extract import extract_from_csv
    from clean import clean_orders

    raw_df = extract_from_csv(os.path.join(os.path.dirname(__file__), "..", "data", "raw_orders_messy.csv"))
    clean_df = clean_orders(raw_df)

    engine = get_postgres_engine(host="localhost")
    load_to_postgres(clean_df, table_name="orders_clean", engine=engine, if_exists="replace")

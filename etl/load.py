import os

import pandas as pd
from sqlalchemy import create_engine


def get_postgres_engine(host="postgres", port=5432, db="warehouse"):
    """Build a database connection. host is the docker service name, not localhost."""
    user = os.getenv("POSTGRES_USER", "de_user")
    password = os.getenv("POSTGRES_PASSWORD", "de_password")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def load_to_postgres(df, table_name, engine, if_exists="append"):
    """Write a DataFrame to a Postgres table. append adds rows, replace wipes the table first."""
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"[load] Wrote {len(df)} rows to table '{table_name}'")


if __name__ == "__main__":
    from extract import extract_from_csv
    from clean import clean_orders

    data_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw_orders_messy.csv")

    raw_df = extract_from_csv(data_path)
    clean_df = clean_orders(raw_df)

    engine = get_postgres_engine(host="localhost")
    load_to_postgres(clean_df, "orders_clean", engine, if_exists="replace")

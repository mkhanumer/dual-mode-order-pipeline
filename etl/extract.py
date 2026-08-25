import os
import pandas as pd


def extract_from_csv(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    print(f"[extract] Read {len(df)} raw rows from {file_path}")
    return df


if __name__ == "__main__":
    raw_df = extract_from_csv(os.path.join(os.path.dirname(__file__), "..", "data", "raw_orders_messy.csv"))
    print(raw_df.head(10))
    print(raw_df.dtypes)

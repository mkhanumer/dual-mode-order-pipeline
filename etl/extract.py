import os

import pandas as pd


def extract_from_csv(file_path):
    """Read a raw CSV file into a DataFrame. No cleaning happens here."""
    df = pd.read_csv(file_path)
    print(f"[extract] Read {len(df)} rows from {file_path}")
    return df


if __name__ == "__main__":
    data_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw_orders_messy.csv")
    raw_df = extract_from_csv(data_path)
    print(raw_df.head(10))
    print(raw_df.dtypes)

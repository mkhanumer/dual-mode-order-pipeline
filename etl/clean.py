import pandas as pd
import numpy as np


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def clean_text_columns(df: pd.DataFrame, text_columns: list) -> pd.DataFrame:
    df = df.copy()
    for col in text_columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.title()
            .replace({"Nan": np.nan, "": np.nan, "None": np.nan})
        )
    return df


def fix_numeric_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.copy()
    df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def fix_date_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.copy()
    df[column] = pd.to_datetime(df[column], format="mixed", errors="coerce")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    before = len(df)

    # drop rows missing critical fields, fill non-critical with defaults
    df = df.dropna(subset=["customer_name", "unit_price"])
    df["country"] = df["country"].fillna("Unknown")

    print(f"[clean] Dropped {before - len(df)} rows with missing critical fields")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    print(f"[clean] Removed {before - len(df)} duplicate rows")
    return df


def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    before = len(df)

    df = df[df["unit_price"] > 0]
    df = df[df["quantity"] > 0]

    print(f"[clean] Removed {before - len(df)} rows that broke business rules (bad price/quantity)")
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["total_amount"] = (df["unit_price"] * df["quantity"]).round(2)
    df["order_year_month"] = df["order_date"].dt.to_period("M").astype(str)
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Run all cleaning steps in order. This is the single entry point the rest of the project calls."""
    df = standardize_column_names(df)
    df = clean_text_columns(df, text_columns=["customer_name", "product_name", "category", "country"])
    df = fix_numeric_column(df, "unit_price")
    df = fix_numeric_column(df, "quantity")
    df = fix_date_column(df, "order_date")
    df = handle_missing_values(df)
    df = remove_duplicates(df)
    df = apply_business_rules(df)
    df = add_derived_columns(df)
    df = df.reset_index(drop=True)
    return df


if __name__ == "__main__":
    from extract import extract_from_csv

    raw_df = extract_from_csv("../data/raw_orders_messy.csv")
    print(f"\nRAW: {len(raw_df)} rows\n")

    clean_df = clean_orders(raw_df)
    print(f"\nCLEAN: {len(clean_df)} rows\n")
    print(clean_df.head(10))
    print(clean_df.dtypes)

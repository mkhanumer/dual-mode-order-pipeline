import pandas as pd
import numpy as np


def standardize_column_names(df):
    """Make all column names lowercase with underscores."""
    df = df.copy()
    new_names = []
    for col in df.columns:
        name = col.strip()
        name = name.lower()
        name = name.replace(" ", "_")
        new_names.append(name)
    df.columns = new_names
    return df


def clean_text_columns(df, text_columns):
    """Trim whitespace and make casing consistent (e.g. '  LAPTOP  ' -> 'Laptop')."""
    df = df.copy()
    for col in text_columns:
        text = df[col].astype(str)
        text = text.str.strip()
        text = text.str.title()

        # empty strings and the string "Nan" should be treated as missing values
        text = text.replace({"Nan": np.nan, "": np.nan, "None": np.nan})
        df[col] = text
    return df


def fix_numeric_column(df, column):
    """Convert a column to numbers. Bad values become NaN instead of crashing."""
    df = df.copy()
    df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def fix_date_column(df, column):
    """Convert a column to dates. Handles files that mix date formats."""
    df = df.copy()
    df[column] = pd.to_datetime(df[column], format="mixed", errors="coerce")
    return df


def handle_missing_values(df):
    """Drop rows missing critical fields, fill non-critical ones."""
    df = df.copy()
    rows_before = len(df)

    # no customer or price means we can't use this order at all
    df = df.dropna(subset=["customer_name", "unit_price"])

    # country is not needed to calculate revenue, so keep the row
    df["country"] = df["country"].fillna("Unknown")

    rows_dropped = rows_before - len(df)
    print(f"[clean] Dropped {rows_dropped} rows missing customer_name or unit_price")
    return df


def remove_duplicates(df):
    """Remove repeated order_id entries (from retries / re-sent files)."""
    df = df.copy()
    rows_before = len(df)
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    removed = rows_before - len(df)
    print(f"[clean] Removed {removed} duplicate rows")
    return df


def apply_business_rules(df):
    """Remove orders that make no sense: negative/zero price or quantity."""
    df = df.copy()
    rows_before = len(df)

    valid_price = df["unit_price"] > 0
    valid_quantity = df["quantity"] > 0
    df = df[valid_price & valid_quantity]

    removed = rows_before - len(df)
    print(f"[clean] Removed {removed} rows with invalid price or quantity")
    return df


def add_derived_columns(df):
    """Add columns that downstream tools will need anyway."""
    df = df.copy()
    df["total_amount"] = df["unit_price"] * df["quantity"]
    df["total_amount"] = df["total_amount"].round(2)

    # "2026-05" style string, useful for grouping by month later
    month_period = df["order_date"].dt.to_period("M")
    df["order_year_month"] = month_period.astype(str)
    return df


def clean_orders(df):
    """Run every cleaning step in order. Entry point used by batch and streaming."""
    df = standardize_column_names(df)
    df = clean_text_columns(df, ["customer_name", "product_name", "category", "country"])
    df = fix_numeric_column(df, "unit_price")
    df = fix_numeric_column(df, "quantity")
    df = fix_date_column(df, "order_date")
    df = handle_missing_values(df)
    df = remove_duplicates(df)
    df = apply_business_rules(df)
    df = add_derived_columns(df)
    return df.reset_index(drop=True)


if __name__ == "__main__":
    from extract import extract_from_csv

    raw_df = extract_from_csv("../data/raw_orders_messy.csv")

    print(f"\nRAW: {len(raw_df)} rows\n")
    clean_df = clean_orders(raw_df)
    print(f"\nCLEAN: {len(clean_df)} rows\n")
    print(clean_df.head(10))
    print(clean_df.dtypes)

import pandas as pd
import numpy as np

from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log

def _snake_case(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to snake_case."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def transform_transactions(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming Transactions...")
    df = _snake_case(df.copy())

    df["has_missing_distributor"] = df["distributor_id"].isnull()
    null_count = df["has_missing_distributor"].sum()
    log.warning(f"  {null_count} transactions have NULL distributor_id — flagged")

    # Coalesce notes nulls
    df["notes"] = df["notes"].fillna("")

    # Standardise transaction_status casing
    df["transaction_status"] = df["transaction_status"].str.strip().str.title()

    # Validate transaction_status values
    valid_statuses = {"Completed", "Returned", "Pending"}
    unexpected = set(df["transaction_status"].unique()) - valid_statuses
    if unexpected:
        log.warning(f"  Unexpected transaction_status values: {unexpected}")

    # Re-derive computed columns to catch any formula errors in source
    df["discount_amount_ngn"] = (
        df["quantity"] * df["unit_price_ngn"] * df["discount_pct"] / 100
    ).round(2)
    df["revenue_ngn"] = (
        df["quantity"] * df["unit_price_ngn"] - df["discount_amount_ngn"]
    ).round(2)
    df["gross_profit_ngn"] = (df["revenue_ngn"] - df["cogs_ngn"]).round(2)

    # Cast types
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    df["discount_pct"] = df["discount_pct"].astype(float)

    # Add ingestion metadata
    df["_is_returned"] = df["transaction_status"] == "Returned"
    df["_loaded_at"] = pd.Timestamp.utcnow()

    log.info(f"  Transactions clean: {len(df)} rows")
    return df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming Products...")
    df = _snake_case(df.copy())
    df["is_active"] = df["is_active"].astype(bool)
    df["_loaded_at"] = pd.Timestamp.utcnow()
    log.info(f"  Products clean: {len(df)} rows")
    return df


def transform_distributors(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming Distributors...")
    df = _snake_case(df.copy())
    df["onboarding_date"] = pd.to_datetime(df["onboarding_date"])
    df["is_active"] = df["is_active"].astype(bool)
    df["region"] = df["region"].str.strip()
    df["_loaded_at"] = pd.Timestamp.utcnow()
    log.info(f"  Distributors clean: {len(df)} rows")
    return df


def transform_salespersons(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming Salespersons...")
    df = _snake_case(df.copy())
    df["hire_date"] = pd.to_datetime(df["hire_date"])
    df["region"] = df["region"].str.strip()
    df["_loaded_at"] = pd.Timestamp.utcnow()
    log.info(f"  Salespersons clean: {len(df)} rows")
    return df


def transform_monthly_targets(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming Monthly_Targets...")
    df = _snake_case(df.copy())

    # Compute missing achievement_pct
    df["achievement_pct"] = np.where(
        df["target_revenue_ngn"] > 0,
        (df["actual_revenue_ngn"] / df["target_revenue_ngn"] * 100).round(2),
        np.nan,  # avoid div by zero
    )
    log.info(f"  Computed achievement_pct for {len(df)} rows")

    df["_loaded_at"] = pd.Timestamp.utcnow()
    return df


def transform_date_table(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming Date_Table...")
    df = _snake_case(df.copy())
    df["date"] = pd.to_datetime(df["date"])
    df["is_weekend"] = df["is_weekend"].astype(bool)
    df["is_month_end"] = df["is_month_end"].astype(bool)
    df["_loaded_at"] = pd.Timestamp.utcnow()
    log.info(f"  Date_Table clean: {len(df)} rows")
    return df


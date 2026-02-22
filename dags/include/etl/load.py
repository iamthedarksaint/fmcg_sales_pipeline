import pandas as pd
from sqlalchemy import create_engine, text

from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log


TABLE_CONFIG = {
    "dim_products": {
        "pk": "product_id",
        "schema": "raw",
        "incremental": False,  
    },
    "dim_distributors": {
        "pk": "distributor_id",
        "schema": "raw",
        "incremental": False,
    },
    "dim_salespersons": {
        "pk": "salesperson_id",
        "schema": "raw",
        "incremental": False,
    },
    "dim_date": {
        "pk": "date",
        "schema": "raw",
        "incremental": False,
    },
    "fct_transactions": {
        "pk": "transaction_id",
        "schema": "raw",
        "incremental": True,  
    },
    "fct_monthly_targets": {
        "pk": "record_id",
        "schema": "raw",
        "incremental": True,
    },
}


DF_TO_TABLE = {
    "transactions": "fct_transactions",
    "products": "dim_products",
    "distributors": "dim_distributors",
    "salespersons": "dim_salespersons",
    "monthly_targets": "fct_monthly_targets",
    "date_table": "dim_date",
}


def get_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)


def ensure_schema(engine, schema: str = "raw"):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()


def get_existing_pks(engine, table: str, schema: str, pk: str) -> set:
    """Fetch existing primary keys to support incremental loads."""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT {pk} FROM {schema}.{table}")
            )
            return {row[0] for row in result}
    except Exception:
        return set() 


def load_table(
    engine,
    df: pd.DataFrame,
    table_name: str,
    config: dict,
):
    schema = config["schema"]
    pk = config["pk"]
    incremental = config["incremental"]

    if incremental:
        existing_pks = get_existing_pks(engine, table_name, schema, pk)
        if existing_pks:
            before = len(df)
            df = df[~df[pk].isin(existing_pks)]
            log.info(
                f"  [{table_name}] Incremental: {before - len(df)} rows already exist, "
                f"loading {len(df)} new rows"
            )
        else:
            log.info(f"  [{table_name}] No existing data, full load: {len(df)} rows")
    else:
        log.info(f"  [{table_name}] Full replace: {len(df)} rows")

    if df.empty:
        log.info(f"  [{table_name}] Nothing new to load, skipping")
        return

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append" if incremental else "replace",
        index=False,
        method="multi",
        chunksize=500,
    )
    log.info(f"  [{table_name}] Loaded successfully")


def load_all(cleaned: dict[str, pd.DataFrame], db_url: str):
    engine = get_engine(db_url)
    ensure_schema(engine)

    load_order = [
        "products",
        "distributors",
        "salespersons",
        "date_table",
        "transactions",
        "monthly_targets",
    ]

    for df_name in load_order:
        table_name = DF_TO_TABLE[df_name]
        config = TABLE_CONFIG[table_name]
        df = cleaned[df_name]
        log.info(f"Loading {df_name} → raw.{table_name}")
        load_table(engine, df, table_name, config)

    log.info("All tables loaded successfully")
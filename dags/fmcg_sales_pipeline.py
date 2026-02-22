from airflow.sdk import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
import pandas as pd
from include.etl.extract import extract_all_sheets
from include.etl.transform import (
    transform_transactions, 
    transform_products,
    transform_distributors,
    transform_salespersons,
    transform_monthly_targets,
    transform_date_table)
from include.etl.load import DF_TO_TABLE, DF_TO_TABLE, TABLE_CONFIG, get_engine, ensure_schema, load_table
from include.config import config

log = LoggingMixin().log

@dag(
    dag_id="fmcg_sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
    )
def fmcg_sales_pipeline():
    
    @task
    def _extract_data():
        data = extract_all_sheets(config.RAW_DATA_PATH)
        return data
    
    @task
    def _transform_data(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        cleaned = {
            "transactions": transform_transactions(raw["Transactions"]),
            "products": transform_products(raw["Products"]),
            "distributors": transform_distributors(raw["Distributors"]),
            "salespersons": transform_salespersons(raw["Salespersons"]),
            "monthly_targets": transform_monthly_targets(raw["Monthly_Targets"]),
            "date_table": transform_date_table(raw["Date_Table"]),
        }
        return cleaned

    @task
    def _load_data(cleaned: dict[str, pd.DataFrame], db_url: str):
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
            log.info(f"Loading {df_name} into raw.{table_name}")
            load_table(engine, df, table_name, config)

        log.info("All tables loaded successfully")
    

    extract_data = _extract_data()
    cleaned_data = _transform_data(extract_data)
    load_table = _load_data(cleaned_data, config.DB_URL)

    extract_data >> cleaned_data >> load_table

fmcg_sales_pipeline()
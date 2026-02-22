from airflow.sdk import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.standard.operators.bash import BashOperator

from datetime import datetime, timedelta
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
from include.notifications.notifications import on_failure_alert

log = LoggingMixin().log

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_alert,
}

@dag(
    dag_id="fmcg_sales_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["fmcg", "sales", "etl"],
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
        result = {}
        for key, df in cleaned.items():
            for col in df.columns:
                if df[col].dtype == "datetime64[ns]" or df[col].dtype == "object":
                    df[col] = df[col].apply(
                        lambda x: x.isoformat() if hasattr(x, "isoformat") else x
                    )
            result[key] = df.to_dict(orient="records")
        
        return result

    @task
    def _load_data(cleaned: dict):
        db_url = config.DB_URL

        cleaned_dfs = {key: pd.DataFrame(records) for key, records in cleaned.items()}

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
            table_config = TABLE_CONFIG[table_name]
            df = cleaned_dfs[df_name]
            log.info(f"Loading {df_name} into raw.{table_name}")
            load_table(engine, df, table_name, table_config)

        log.info("All tables loaded successfully")

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir . --project-dir . "
            "--target dev"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --profiles-dir . --project-dir . "
            "--target dev"
        ),
    )

    

    extract_data = _extract_data()
    cleaned_data = _transform_data(extract_data)
    load_data = _load_data(cleaned_data)

    load_data >> dbt_run >> dbt_test

fmcg_sales_pipeline()
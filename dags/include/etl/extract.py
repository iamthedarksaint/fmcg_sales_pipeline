import pandas as pd  
from datetime import datetime
from typing import Dict
from include.config import config

from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

EXPECTED_SHEETS = [
    "Transactions",
    "Products",
    "Distributors",
    "Salespersons",
    "Monthly_Targets",
    "Date_Table",
]

def extract_all_sheets(file_path: str) -> Dict[str, pd.DataFrame]:
    try:
        log.info(f"Extracting data from {file_path} at {datetime.now()}")
        data = pd.read_excel(file_path, sheet_name=None)
        sheet_names = list(data.keys())[1:]
        data = {sheet_name: data[sheet_name] for sheet_name in sheet_names}

        missing_sheets = [sheet for sheet in EXPECTED_SHEETS if sheet not in data]
        if missing_sheets:
            log.warning(f"Missing expected sheets: {missing_sheets}")

        for name, df in data.items():
            log.info(f"{name}: {df.shape[0]} rows x {df.shape[1]} x columns)")
        return data
    except Exception as e:
        log.error(f"Error during data extraction: {e}")

extract_all_sheets(config.RAW_DATA_PATH)
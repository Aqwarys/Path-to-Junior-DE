from pathlib import Path
from datetime import datetime
from logger import log

import pandas as pd


def load_data(output_data: pd.DataFrame, PROCESSED_PATH: Path):
    log.info(f"Saving data to {PROCESSED_PATH} started")
    output_file_name = PROCESSED_PATH / f"cleaned_{datetime.now().strftime('%Y%m%d')}.csv"

    output_data.to_csv(output_file_name, index=False)
    log.info(f"Saved processed data: {output_file_name}")
    return output_file_name

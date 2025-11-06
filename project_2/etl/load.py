import pandas as pd

from pathlib import Path
from datetime import datetime
from uuid import uuid4

from logger import log





def load_data(output_data: pd.DataFrame, PROCESSED_PATH: Path):
    log.info(f"Saving data to {PROCESSED_PATH} started")
    output_file_name = PROCESSED_PATH / f"cleaned_{datetime.now().strftime('%Y%m%d')}-{uuid4()}.csv"

    output_data.to_csv(output_file_name, index=False)
    log.info(f"Saved processed data: {output_file_name}")
    return output_file_name
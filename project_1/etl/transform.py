import pandas as pd

from pathlib import Path

from utils.cleaner import clean_anime_data, data_parsing
from logger import log



def transform_data(raw_file_path: Path):
    log.info(f"Started Transforming data from {raw_file_path}")
    df = pd.read_json(raw_file_path, orient="records", encoding='utf-8')
    log.info(f"Transform and cleaning data")
    df = clean_anime_data(data_parsing(df))
    log.info("Raw data transformed and cleaned")

    return df
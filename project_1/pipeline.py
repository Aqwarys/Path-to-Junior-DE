from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

from logger import log

from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

RAW_PATH = Path(str(os.environ.get('RAW_PATH')))
RAW_PATH.mkdir(parents=True, exist_ok=True)
PRCESSED_PATH = Path(str(os.environ.get('PRCESSED_PATH')))
PRCESSED_PATH.mkdir(parents=True, exist_ok=True)
URL = str(os.environ.get('URL'))


def run_pipeline():
    log.info("Starting ETL pipeline")
    raw_file_path = extract_data(RAW_PATH=RAW_PATH, URL=URL)
    if not raw_file_path:
        log.info("❌ Extraction failed")
        return
    df = transform_data(raw_file_path=raw_file_path)
    output = load_data(output_data=df, PROCESSED_PATH=PRCESSED_PATH)

    log.info(f"Pipeline completed successfully: {output}")


if __name__ == "__main__":
    run_pipeline()
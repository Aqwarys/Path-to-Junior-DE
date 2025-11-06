import os
from pathlib import Path

from logger import log
from dotenv import load_dotenv

from etl.extract import extract_data
from etl.transform import data_cleaning
from etl.load import load_data

load_dotenv()

RAW_PATH = Path(str(os.environ.get("RAW_PATH")))
RAW_PATH.mkdir(parents=True, exist_ok=True)
PROCESSED_PATH = Path(str(os.environ.get("PROCESSED_PATH")))
PROCESSED_PATH.mkdir(parents=True, exist_ok=True)
URL = str(os.environ.get("URL"))
if not URL:
    raise ValueError("URL is not set")


def run_pipeline():
    log.info("Starting ETL pipeline")
    pages = int(input("Enter number of pages to scrape: "))
    file_path = extract_data(RAW_PATH=RAW_PATH, URL=URL, pages=pages)
    log.info("Data successfully extracted from API")
    df = data_cleaning(file_path=file_path)
    log.info("Data successfully cleaned")
    output = load_data(output_data=df, PROCESSED_PATH=PROCESSED_PATH)
    log.info("Pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()
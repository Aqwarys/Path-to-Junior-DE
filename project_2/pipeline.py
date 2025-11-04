import os
from pathlib import Path

from logger import log
from dotenv import load_dotenv

from etl.extract import extract_data

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
    extract_data(RAW_PATH=RAW_PATH, URL=URL, pages=pages)
    log.info("Pipeline completed successfully")

if __name__ == "__main__":
    run_pipeline()
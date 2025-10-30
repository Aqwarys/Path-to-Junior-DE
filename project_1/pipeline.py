from etl.extract import extract_data
from etl.transform import transform_data
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
    raw_file_path = extract_data(RAW_PATH=RAW_PATH, URL=URL)
    transform_data(raw_file_path=raw_file_path)

print(run_pipeline())
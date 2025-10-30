from etl.extract import extract_data
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
    return extract_data(RAW_PATH=RAW_PATH, URL=URL)

print(run_pipeline())
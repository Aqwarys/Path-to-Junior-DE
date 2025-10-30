import requests
from pathlib import Path
import os
import time
from datetime import date

import pandas as pd

from logger import log
from utils import cleaner

def extract_data(RAW_PATH: Path, URL: str):

    """Extract data from API and save it to RAW_PATH"""

    log.info(f"Started Extracting data from {URL} to {RAW_PATH}")
    response = requests.get(URL)
    response.raise_for_status()
    response = response.json()

    log.info(f"Extracting pages from {URL}")
    page = 1
    while response["pagination"]["has_next_page"] and page < 3:
        # time.sleep(0.5)
        if "?" in URL:
            response_page = requests.get(f"{URL}&page={response["pagination"]["current_page"] + 1}")
        else:
            response_page = requests.get(f"{URL}?page={response["pagination"]["current_page"] + 1}")

        response_page.raise_for_status()
        response_page = response_page.json()

        response["data"] += response_page["data"]
        response["pagination"]["has_next_page"] = response_page["pagination"]["has_next_page"]
        page += 1
    log.info(f"Extracted {page} pages from {URL}")

    log.info(f"Saving data to {RAW_PATH}")
    file_path = f"{RAW_PATH}/{date.today().strftime("%Y-%m-%d")}.json"
    pd.DataFrame(response["data"]).to_json(file_path, orient="records",indent=4, force_ascii=False)

    log.info(f"Data saved to {RAW_PATH}")

    return Path(file_path)

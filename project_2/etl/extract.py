from pathlib import Path
import requests
import uuid
from datetime import datetime
import time
import pandas as pd

from logger import log


def extract_data(RAW_PATH: Path, URL: str, pages: int = 1):

    log.info(f"Started Extracting data from {URL} to {RAW_PATH}")

    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()

    log.info(f"Extracting pages from {URL}")

    page = 1
    try:
        while data["pagination"]["has_next_page"] and page <= pages:
            time.sleep(0.5)
            if '?' in URL and data['pagination']['has_next_page']:
                response = requests.get(f"{URL}&page={data["pagination"]["current_page"] + 1}")
            else:
                response = requests.get(f"{URL}?page={data["pagintation"]["current_page"] + 1}")
            response.raise_for_status()
            response = response.json()
            data["data"] += response["data"]
            data["pagination"]["has_next_page"] = response["pagination"]["has_next_page"]
            data["pagination"]["current_page"] = response["pagination"]["current_page"]
            page += 1
    except Exception as e:
        log.info(f"Error: {e}")
        return None

    data = data["data"]
    file_name = f"{datetime.today().strftime('%Y-%m-%d %H-%M-%S')}-{uuid.uuid4()}.json"
    file_path = RAW_PATH / file_name

    log.info(f"Saving data to {file_path}")
    pd.DataFrame(data).to_json(file_path, orient="records", indent=4, force_ascii=False)

    log.info(f"Extracted data saved to {file_path}")
    return file_path

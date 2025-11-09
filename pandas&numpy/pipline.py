from pathlib import Path
import datetime

import pandas as pd

from logger import logging


BASEURL = 'data'
RAWPATH = Path(BASEURL) / 'raw'
PROCESSED = Path(BASEURL) / 'processed'

logging.info(f"Processing data from {RAWPATH} to {PROCESSED}")

csv_files = [f for f in RAWPATH.rglob('*.csv')]

logging.info(f"Found {len(csv_files)} files to process")

if csv_files:
    logging.info(f"Processing {len(csv_files), [f"{f.name} - {f.stat().st_size} bytes" for f in csv_files]} files")
    df = pd.concat(map(pd.read_csv, csv_files), ignore_index=True)
    logging.info(f"Processed {len(df)} rows and starting createing new file in {PROCESSED}")
    file_name = f'{datetime.datetime.now().strftime("%Y-%m-%d")}_data.csv'
    logging.info(f"Removing duplicates")
    df.drop_duplicates(inplace=True)
    logging.info(f"Saving data to {PROCESSED / f'{file_name}'}")
    if not PROCESSED.exists():
        logging.info(f"Creating {PROCESSED} directory cause it doesn't exist")
        PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED / f'{file_name}', index=False)
    logging.info(f"Data saved to {PROCESSED / f'{file_name}'}")

    print(f"Data saved to {PROCESSED / f'{file_name}'}")
else:
    print("No data to process")
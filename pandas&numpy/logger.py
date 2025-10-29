import logging
from pathlib import Path

logging.basicConfig(
    filename=Path("logs/pipline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
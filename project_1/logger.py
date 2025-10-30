import logging
from pathlib import Path

LOG_FILE_PATH = Path("logs/pipline.log")

LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

log = logging.getLogger(__name__)
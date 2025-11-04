import logging
from datetime import date
import uuid
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


LOG_PATH = Path(str(os.environ.get("LOG_PATH")))
LOG_PATH.mkdir(parents=True, exist_ok=True)



logging.basicConfig(
    filename=f"{LOG_PATH / 'pipeline'}{date.today().strftime("%Y-%m-%d")}-{uuid.uuid4()}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

log = logging.getLogger(__name__)
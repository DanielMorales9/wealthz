import os
from pathlib import Path

SECRETS_DIR = Path(os.environ["SECRETS_DIR"])
CONFIG_DIR = Path(os.environ["CONFIG_DIR"])
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
DEFAULT_LOG_FORMAT = "[%(asctime)s] %(levelname)s %(filename)s:%(lineno)s - %(message)s"
LOG_FORMAT = os.getenv("LOG_FORMAT", DEFAULT_LOG_FORMAT)
DATETIME_ISO_FORMAT = "%Y-%m-%d %H:%M:%S"

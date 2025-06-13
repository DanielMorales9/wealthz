import os
from pathlib import Path

SECRETS_DIR = Path(os.environ["SECRETS_DIR"])
CONFIG_DIR = Path(os.environ["CONFIG_DIR"])
DUCKDB_LOCAL_META_PATH = os.environ["DUCKDB_LOCAL_META_PATH"]
DUCKDB_LOCAL_DATA_PATH = os.environ["DUCKDB_LOCAL_DATA_PATH"]
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
DEFAULT_LOG_FORMAT = "[%(asctime)s] %(levelname)s %(filename)s:%(lineno)s - %(message)s"
LOG_FORMAT = os.getenv("LOG_FORMAT", DEFAULT_LOG_FORMAT)

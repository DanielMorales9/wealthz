import os
from pathlib import Path

SECRETS_DIR = Path(os.environ["SECRETS_DIR"])
CONFIG_DIR = Path(os.environ["CONFIG_DIR"])
DUCKDB_LOCAL_META_PATH = os.environ["DUCKDB_LOCAL_META_PATH"]
DUCKDB_LOCAL_DATA_PATH = os.environ["DUCKDB_LOCAL_DATA_PATH"]

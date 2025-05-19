import os
from pathlib import Path

SECRETS_DIR = Path(os.environ["SECRETS_DIR"])
CONFIG_DIR = Path(os.environ["CONFIG_DIR"])
DUCKDB_META_PATH = Path(os.environ["DUCKDB_META_PATH"])
DUCKDB_DATA_PATH = Path(os.environ["DUCKDB_DATA_PATH"])

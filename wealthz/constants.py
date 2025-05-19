import os
from pathlib import Path

SECRETS_DIR = Path(os.environ["SECRETS_DIR"])
CONFIG_DIR = Path(os.environ["CONFIG_DIR"])
DUCKDB_LOCAL_META_PATH = Path(os.environ["DUCKDB_LOCAL_META_PATH"])
DUCKDB_LOCAL_DATA_PATH = Path(os.environ["DUCKDB_LOCAL_DATA_PATH"])
DUCKDB_GCS_META_PATH = Path(os.environ["DUCKDB_GCS_META_PATH"])
DUCKDB_GCS_DATA_PATH = Path(os.environ["DUCKDB_GCS_DATA_PATH"])

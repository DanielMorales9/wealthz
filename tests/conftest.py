from pathlib import Path

from dotenv import load_dotenv

# Load test environment early
test_env_path = Path(__file__).parent.parent / ".env.test"
load_dotenv(dotenv_path=test_env_path, override=True)


GSHEET_ETL_PIPELINE = {
    "engine": {"type": "duckdb"},
    "name": "test_name",
    "columns": [
        {"name": "column1", "type": "string"},
        {"name": "column2", "type": "integer"},
    ],
    "datasource": {
        "type": "gsheet",
        "sheet_id": "test-id",
        "sheet_range": "test-sheet-range",
        "credentials_file": "mock-creds.json",
    },
    "primary_keys": ["column1"],
    "replication": "full",
}

DUCKLAKE_ETL_PIPELINE = {
    "engine": {"type": "duckdb"},
    "name": "test_ducklake",
    "columns": [
        {"name": "id", "type": "integer"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "float"},
    ],
    "datasource": {
        "type": "ducklake",
        "query": "SELECT id, name, amount FROM test_table",
    },
    "primary_keys": ["id"],
    "replication": "full",
}

YFINANCE_ETL_PIPELINE = {
    "engine": {"type": "duckdb"},
    "name": "test_ticker",
    "columns": [
        {"name": "Date", "type": "date"},
        {"name": "Open", "type": "float"},
        {"name": "High", "type": "float"},
        {"name": "Low", "type": "float"},
        {"name": "Close", "type": "float"},
        {"name": "Adj Close", "type": "float"},
        {"name": "Volume", "type": "integer"},
    ],
    "datasource": {
        "type": "yfinance",
        "symbols": ["AAPL"],
        "period": "1d",
        "interval": "1h",
    },
    "primary_keys": ["Date"],
    "replication": "full",
}

from pathlib import Path

from dotenv import load_dotenv

from wealthz.model import ETLPipeline


def pytest_configure():
    test_env_path = Path(__file__).parent.parent / ".env.test"
    load_dotenv(dotenv_path=test_env_path, override=True)


GSHEET_ETL_PIPELINE = ETLPipeline(
    engine={"type": "duckdb"},
    schema="test_schema",
    name="test_name",
    columns=[
        {"name": "column1", "type": "string"},
        {"name": "column2", "type": "integer"},
    ],
    datasource={
        "type": "gsheet",
        "sheet_id": "test-id",
        "sheet_range": "test-sheet-range",
        "credentials_file": "mock-creds.json",
    },
    primary_keys=["column1"],
    replication="full",
)

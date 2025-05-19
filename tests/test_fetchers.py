from unittest.mock import MagicMock, patch

import polars
import pytest
from google.auth.credentials import Credentials
from polars import DataFrame

from wealthz.fetchers import GoogleSheetFetcher
from wealthz.model import ETLPipeline


@pytest.fixture()
def mock_gsheet():
    with patch("wealthz.fetchers.build") as mock_client_builder:
        mock_build = MagicMock()
        mock_client_builder.return_value = mock_build
        yield mock_build.spreadsheets.return_value


@pytest.mark.parametrize(
    "pipeline, data, expected",
    [
        (
            ETLPipeline(
                engine={"type": "duckdb", "storage": "fs"},
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
            ),
            {
                "values": [["column1", "column2"], ["value1", 123], ["value2", 456]],
            },
            DataFrame(
                {
                    "column1": [
                        "value1",
                        "value2",
                    ],
                    "column2": [123, 456],
                },
                schema={"column1": polars.String, "column2": polars.Int32},
            ),
        )
    ],
)
def test_google_sheet_fetcher(pipeline, data, expected, mock_gsheet):
    mock_gsheet.values.return_value.get.return_value.execute.return_value = data
    mock_credentials = MagicMock(spec=Credentials)
    fetcher = GoogleSheetFetcher(mock_credentials)
    actual = fetcher.fetch(pipeline)
    assert not actual.is_empty()
    assert actual.equals(expected)

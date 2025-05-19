from unittest.mock import MagicMock, patch

import pytest
from google.auth.credentials import Credentials

from wealthz.fetchers import GoogleSheetFetcher
from wealthz.model import ETLPipeline


@pytest.fixture()
def mock_gsheet():
    with patch("wealthz.fetchers.build") as mock_client_builder:
        mock_build = MagicMock()
        mock_client_builder.return_value = mock_build
        yield mock_build.spreadsheets.return_value


@pytest.mark.parametrize(
    "pipeline",
    [
        ETLPipeline(
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
            },
        )
    ],
)
def test_google_sheet_fetcher(pipeline, mock_gsheet):
    mock_gsheet.values.return_value.get.return_value.execute.return_value.get.return_value = {}
    mock_credentials = MagicMock(spec=Credentials)
    fetcher = GoogleSheetFetcher(mock_credentials)
    assert fetcher.fetch(pipeline) is not None

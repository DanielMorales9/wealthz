from unittest.mock import MagicMock, patch

import pytest
from google.auth.credentials import Credentials

from wealthz.fetchers import GoogleSheetFetcher


@pytest.fixture()
def mock_spreadsheet():
    with patch("wealthz.fetchers.build") as mock_client_builder:
        return mock_client_builder.return_value.spreadsheets.return_value


def test_google_sheet_fetcher(mock_spreadsheet):
    mock_spreadsheet.values.return_value.get.return_value.execute.return_value.get.return_value = {}

    mock_credentials = MagicMock(spec=Credentials)
    assert GoogleSheetFetcher("test-id", "test-sheet-range", mock_credentials) is not None

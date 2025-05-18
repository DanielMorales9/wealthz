from pathlib import Path
from unittest.mock import MagicMock, patch

from wealthz.secrets import SPREADSHEETS_SCOPES, GoogleCredentialsManager, GoogleCredentialsScope


@patch("wealthz.secrets.Credentials.from_service_account_file")
def test_create_credentials_with_spreadsheet_scope(mock_from_file):
    # Arrange
    fake_creds = MagicMock()
    mock_from_file.return_value = fake_creds

    manager = GoogleCredentialsManager(path=Path("/fake/path/to/creds.json"), scope=GoogleCredentialsScope.SPREADSHEETS)

    # Act
    creds = manager.create()

    # Assert
    assert creds is fake_creds
    mock_from_file.assert_called_once_with(Path("/fake/path/to/creds.json"), scopes=SPREADSHEETS_SCOPES)


@patch("wealthz.secrets.Credentials.from_service_account_file")
def test_create_credentials_with_no_scope(mock_from_file):
    # Arrange
    fake_creds = MagicMock()
    mock_from_file.return_value = fake_creds

    manager = GoogleCredentialsManager(path=Path("/fake/path/to/creds.json"), scope=None)

    # Act
    creds = manager.create()

    # Assert
    assert creds is fake_creds
    mock_from_file.assert_called_once_with(Path("/fake/path/to/creds.json"), scopes=[])

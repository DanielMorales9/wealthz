from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from tests.conftest import GSHEET_ETL_PIPELINE
from wealthz.factories import (
    SPREADSHEETS_SCOPES,
    SPREADSHEETS_WRITE_SCOPES,
    GoogleCredentialsFactory,
    GoogleCredentialsScope,
    GoogleSheetsLoaderFactory,
    LoaderFactory,
    TransformerFactory,
    UnknownDestinationTypeError,
)
from wealthz.loaders import DuckLakeLoader, GoogleSheetsLoader
from wealthz.model import (
    Column,
    ColumnType,
    DestinationType,
    DuckLakeDestination,
    ETLPipeline,
    GoogleSheetDestination,
    TrimTransform,
)
from wealthz.transforms import ColumnTransformer, NoopTransformer


@patch("wealthz.factories.Credentials.from_service_account_file")
def test_create_credentials_with_spreadsheet_scope(mock_from_file):
    # Arrange
    fake_creds = MagicMock()
    mock_from_file.return_value = fake_creds

    manager = GoogleCredentialsFactory("creds.json", scope=GoogleCredentialsScope.SPREADSHEETS)

    # Act
    creds = manager.create()

    # Assert
    assert creds is fake_creds
    mock_from_file.assert_called_once_with(Path("tmp/secrets/creds.json"), scopes=SPREADSHEETS_SCOPES)


@patch("wealthz.factories.Credentials.from_service_account_file")
def test_create_credentials_with_no_scope(mock_from_file):
    # Arrange
    fake_creds = MagicMock()
    mock_from_file.return_value = fake_creds

    manager = GoogleCredentialsFactory("creds.json", scope=None)

    # Act
    creds = manager.create()

    # Assert
    assert creds is fake_creds
    mock_from_file.assert_called_once_with(Path("tmp/secrets/creds.json"), scopes=[])


class TestTransformerFactory:
    def test_create_column_transformer_when_pipeline_has_transforms(self):
        # Arrange
        column_with_transforms = Column(name="test_column", type=ColumnType.STRING, transforms=[TrimTransform()])
        pipeline = MagicMock(spec=ETLPipeline)
        pipeline.has_transforms = True
        pipeline.columns = [column_with_transforms]

        factory = TransformerFactory(pipeline)

        # Act
        transformer = factory.create()

        # Assert
        assert isinstance(transformer, ColumnTransformer)

    def test_create_noop_transformer_when_pipeline_has_no_transforms(self):
        # Arrange
        pipeline = MagicMock(spec=ETLPipeline)
        pipeline.has_transforms = False

        factory = TransformerFactory(pipeline)

        # Act
        transformer = factory.create()

        # Assert
        assert isinstance(transformer, NoopTransformer)


@patch("wealthz.factories.Credentials.from_service_account_file")
def test_create_credentials_with_write_scope(mock_from_file):
    # Arrange
    fake_creds = MagicMock()
    mock_from_file.return_value = fake_creds

    manager = GoogleCredentialsFactory(
        credential_file_name="creds.json", scope=GoogleCredentialsScope.SPREADSHEETS_WRITE
    )

    # Act
    creds = manager.create()

    # Assert
    assert creds is fake_creds
    mock_from_file.assert_called_once_with(Path("tmp/secrets/creds.json"), scopes=SPREADSHEETS_WRITE_SCOPES)


@patch("wealthz.loaders.build")
@patch("wealthz.factories.Credentials.from_service_account_file")
def test_google_sheets_loader_factory(mock_from_file, mock_build):
    # Arrange
    fake_creds = MagicMock()
    mock_from_file.return_value = fake_creds

    factory = GoogleSheetsLoaderFactory(
        sheet_id="test_sheet_id", credentials_file_name="test_creds.json", sheet_range="Sheet1!A1:Z100"
    )

    # Act
    loader = factory.create()

    # Assert
    assert isinstance(loader, GoogleSheetsLoader)
    mock_from_file.assert_called_once()


def test_loader_factory_create_ducklake_loader_default():
    """Test LoaderFactory creates DuckLakeLoader when no destination specified"""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    pipeline = GSHEET_ETL_PIPELINE
    pipeline.destination = None

    loader = LoaderFactory(pipeline, mock_conn).create()

    assert isinstance(loader, DuckLakeLoader)


def test_loader_factory_create_ducklake_loader_explicit():
    """Test LoaderFactory creates DuckLakeLoader when ducklake destination specified"""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    GSHEET_ETL_PIPELINE.destination = DuckLakeDestination(type=DestinationType.DUCKLAKE)

    loader = LoaderFactory(GSHEET_ETL_PIPELINE, mock_conn).create()

    assert isinstance(loader, DuckLakeLoader)


@patch("wealthz.loaders.build")
@patch("wealthz.factories.Credentials.from_service_account_file")
def test_loader_factory_create_google_sheets_loader(mock_from_file, mock_build):
    """Test LoaderFactory creates GoogleSheetsLoader when gsheet destination specified"""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_from_file.return_value = MagicMock()

    GSHEET_ETL_PIPELINE.destination = GoogleSheetDestination(
        type=DestinationType.GOOGLE_SHEET,
        sheet_id="test_sheet_id",
        credentials_file="test_creds.json",
        sheet_range="Sheet1!A1:Z100",
    )

    loader = LoaderFactory(GSHEET_ETL_PIPELINE, mock_conn).create()

    assert isinstance(loader, GoogleSheetsLoader)


def test_loader_factory_unknown_destination_type():
    """Test LoaderFactory raises error for unknown destination type"""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)

    GSHEET_ETL_PIPELINE.destination = MagicMock()
    GSHEET_ETL_PIPELINE.destination.type = "unknown_type"

    with pytest.raises(UnknownDestinationTypeError):
        LoaderFactory(GSHEET_ETL_PIPELINE, mock_conn).create()

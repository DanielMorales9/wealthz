from pathlib import Path
from unittest.mock import MagicMock, patch

from wealthz.factories import (
    SPREADSHEETS_SCOPES,
    GoogleCredentialsFactory,
    GoogleCredentialsScope,
    TransformerFactory,
)
from wealthz.model import Column, ColumnType, ETLPipeline, TrimTransform
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

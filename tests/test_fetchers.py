from unittest.mock import MagicMock, patch

import polars
import pytest
from conftest import GSHEET_ETL_PIPELINE
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
            ETLPipeline(**GSHEET_ETL_PIPELINE),
            {
                "values": [["column1", "column2"], ["value1", "123"], ["value2", "456"]],
            },
            DataFrame(
                {
                    "column1": [
                        "value1",
                        "value2",
                    ],
                    "column2": ["123", "456"],
                },
                schema={"column1": polars.String, "column2": polars.String},
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


def test_google_sheet_fetcher_returns_only_string_columns(mock_gsheet):
    """Test that GoogleSheetFetcher returns dataframe with only string columns"""
    pipeline = ETLPipeline(**GSHEET_ETL_PIPELINE)
    data = {
        "values": [["column1", "column2"], ["text_value", "123"], ["another_text", "456"]],
    }

    mock_gsheet.values.return_value.get.return_value.execute.return_value = data
    mock_credentials = MagicMock(spec=Credentials)
    fetcher = GoogleSheetFetcher(mock_credentials)

    df = fetcher.fetch(pipeline)

    # Verify all columns are strings
    for column_name in df.columns:
        assert (
            df[column_name].dtype == polars.String
        ), f"Column {column_name} should be String but is {df[column_name].dtype}"

    # Verify data is preserved as strings
    assert df["column1"].to_list() == ["text_value", "another_text"]
    assert df["column2"].to_list() == ["123", "456"]

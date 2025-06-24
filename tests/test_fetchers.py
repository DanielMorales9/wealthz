from datetime import datetime
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import polars
import pytest
import pytz
from conftest import DUCKLAKE_ETL_PIPELINE, GSHEET_ETL_PIPELINE, YFINANCE_ETL_PIPELINE
from google.auth.credentials import Credentials
from polars import DataFrame

from wealthz.fetchers import DuckLakeFetcher, GoogleSheetFetcher, YFinanceFetcher
from wealthz.model import ETLPipeline


@pytest.fixture()
def mock_gsheet():
    with patch("wealthz.fetchers.build") as mock_client_builder:
        mock_build = MagicMock()
        mock_client_builder.return_value = mock_build
        yield mock_build.spreadsheets.return_value


@pytest.fixture()
def test_dataframe():
    return DataFrame({"id": [1], "name": ["test"], "amount": [100.0]})


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


@pytest.fixture()
def mock_duckdb_conn():
    """Create a mock DuckDB connection for testing."""
    mock_conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    return mock_conn


@pytest.fixture()
def ducklake_pipeline():
    """Create a DuckLake ETL pipeline for testing."""
    return ETLPipeline(**DUCKLAKE_ETL_PIPELINE)


def test_ducklake_fetcher_fetch_success(mock_duckdb_conn, ducklake_pipeline):
    """Test successful data fetching from DuckLakeFetcher."""
    # Setup mock return data
    expected_df = DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "amount": [100.0, 200.0, 300.0]})

    # Mock the DuckDB execution chain
    mock_result = MagicMock()
    mock_result.pl.return_value = expected_df
    mock_duckdb_conn.execute.return_value = mock_result

    # Create fetcher and test
    fetcher = DuckLakeFetcher(mock_duckdb_conn)
    result = fetcher.fetch(ducklake_pipeline)

    # Verify the query was executed
    mock_duckdb_conn.execute.assert_called_once_with("SELECT id, name, amount FROM test_table")
    mock_result.pl.assert_called_once()

    # Verify the result
    assert result.equals(expected_df)


def test_ducklake_fetcher_fetch_empty_result(mock_duckdb_conn, ducklake_pipeline):
    """Test DuckLakeFetcher with empty query result."""
    # Setup mock return empty data
    expected_df = DataFrame(
        {"id": [], "name": [], "amount": []},
        schema={"id": polars.Int64, "name": polars.String, "amount": polars.Float64},
    )

    # Mock the DuckDB execution chain
    mock_result = MagicMock()
    mock_result.pl.return_value = expected_df
    mock_duckdb_conn.execute.return_value = mock_result

    # Create fetcher and test
    fetcher = DuckLakeFetcher(mock_duckdb_conn)
    result = fetcher.fetch(ducklake_pipeline)

    # Verify the query was executed
    mock_duckdb_conn.execute.assert_called_once_with("SELECT id, name, amount FROM test_table")

    # Verify the result is empty but has correct schema
    assert result.is_empty()
    assert result.columns == ["id", "name", "amount"]


def test_ducklake_fetcher_query_execution_error(mock_duckdb_conn, ducklake_pipeline):
    """Test DuckLakeFetcher handling of query execution errors."""
    # Setup mock to raise exception
    mock_duckdb_conn.execute.side_effect = duckdb.Error("Table 'test_table' not found")

    # Create fetcher and test
    fetcher = DuckLakeFetcher(mock_duckdb_conn)

    # Verify exception is raised
    with pytest.raises(duckdb.Error, match="Table 'test_table' not found"):
        fetcher.fetch(ducklake_pipeline)

    # Verify the query was attempted
    mock_duckdb_conn.execute.assert_called_once_with("SELECT id, name, amount FROM test_table")


@patch("wealthz.fetchers.logging")
def test_ducklake_fetcher_logs_query(mock_logging, mock_duckdb_conn, ducklake_pipeline, test_dataframe):
    """Test that DuckLakeFetcher logs the query being executed."""
    # Setup mock return data
    expected_df = test_dataframe
    mock_result = MagicMock()
    mock_result.pl.return_value = expected_df
    mock_duckdb_conn.execute.return_value = mock_result

    # Create fetcher and test
    fetcher = DuckLakeFetcher(mock_duckdb_conn)
    fetcher.fetch(ducklake_pipeline)

    # Verify logging calls
    mock_logging.info.assert_called_with("Fetching data from DuckDB...")
    # Note: The logger.info call uses the module logger, so we need to check that too


@patch("wealthz.fetchers.logger")
def test_ducklake_fetcher_logs_query_details(mock_logger, mock_duckdb_conn, ducklake_pipeline, test_dataframe):
    """Test that DuckLakeFetcher logs query details."""
    # Setup mock return data
    expected_df = test_dataframe
    mock_result = MagicMock()
    mock_result.pl.return_value = expected_df
    mock_duckdb_conn.execute.return_value = mock_result

    # Create fetcher and test
    fetcher = DuckLakeFetcher(mock_duckdb_conn)
    fetcher.fetch(ducklake_pipeline)

    # Verify query details are logged
    mock_logger.info.assert_called_with("Query: %s", "SELECT id, name, amount FROM test_table")


def test_ducklake_fetcher_different_query(mock_duckdb_conn):
    """Test DuckLakeFetcher with a different query."""
    # Create pipeline with different query
    pipeline_config = DUCKLAKE_ETL_PIPELINE.copy()
    pipeline_config["datasource"]["query"] = "SELECT * FROM users WHERE active = true"
    pipeline = ETLPipeline(**pipeline_config)

    # Setup mock return data
    expected_df = DataFrame({"id": [1], "name": ["active_user"], "amount": [50.0]})
    mock_result = MagicMock()
    mock_result.pl.return_value = expected_df
    mock_duckdb_conn.execute.return_value = mock_result

    # Create fetcher and test
    fetcher = DuckLakeFetcher(mock_duckdb_conn)
    result = fetcher.fetch(pipeline)

    # Verify the correct query was executed
    mock_duckdb_conn.execute.assert_called_once_with("SELECT * FROM users WHERE active = true")
    assert result.equals(expected_df)


@pytest.mark.parametrize(
    "query, expected_data",
    [
        (
            "SELECT COUNT(*) as total FROM orders",
            DataFrame({"total": [42]}),
        ),
        (
            "SELECT product_id, SUM(quantity) as total_qty FROM sales GROUP BY product_id",
            DataFrame({"product_id": [1, 2], "total_qty": [10, 25]}),
        ),
    ],
)
def test_ducklake_fetcher_various_queries(mock_duckdb_conn, query, expected_data):
    """Test DuckLakeFetcher with various query types."""
    # Create pipeline with custom query
    pipeline_config = DUCKLAKE_ETL_PIPELINE.copy()
    pipeline_config["datasource"]["query"] = query
    pipeline = ETLPipeline(**pipeline_config)

    # Setup mock return data
    mock_result = MagicMock()
    mock_result.pl.return_value = expected_data
    mock_duckdb_conn.execute.return_value = mock_result

    # Create fetcher and test
    fetcher = DuckLakeFetcher(mock_duckdb_conn)
    result = fetcher.fetch(pipeline)

    # Verify the query was executed
    mock_duckdb_conn.execute.assert_called_once_with(query)
    assert result.equals(expected_data)


@pytest.fixture()
def ticker_pipeline():
    """Create a Ticker ETL pipeline for testing."""
    return ETLPipeline(**YFINANCE_ETL_PIPELINE)


@patch("wealthz.fetchers.yf")
def test_yfinance_fetcher_retrieves_data(mock_yfinance, ticker_pipeline, test_dataframe):
    """Test that YFinanceFetcher retrieves data from YFinance."""
    expected_df = pd.DataFrame(
        data={"id": [1], "name": ["test"], "amount": [100.0]},
        index=[datetime(2024, 1, 1, 2, 0, 0, tzinfo=pytz.timezone("CET"))],
    )
    fetcher = YFinanceFetcher()
    mock_yfinance.Ticker.return_value = mock_yticker = MagicMock()
    mock_yticker.history.return_value = expected_df
    expected_df = fetcher.fetch(ticker_pipeline)

    assert mock_yfinance.Ticker.called
    assert mock_yticker.history.called
    assert expected_df.columns == ["index", "id", "name", "amount", "Symbol"]

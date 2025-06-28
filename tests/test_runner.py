from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from duckdb import DuckDBPyConnection

from wealthz.model import ETLPipeline
from wealthz.runner import PipelineRunner


@pytest.fixture
def mock_pipeline():
    mock_datasource = MagicMock()
    mock_datasource.type = "yfinance"
    mock_pipeline = MagicMock(spec=ETLPipeline)
    mock_pipeline.name = "test_pipeline"
    mock_pipeline.datasource = mock_datasource
    return mock_pipeline


@pytest.fixture
def mock_conn():
    mock_conn = MagicMock(spec=DuckDBPyConnection)
    return mock_conn


@pytest.fixture
def mock_conn_manager_class():
    with patch("wealthz.runner.DuckLakeConnManager") as mock_conn_manager_class:
        yield mock_conn_manager_class


@pytest.fixture
def mock_conn_manager(mock_conn_manager_class, mock_conn):
    mock_conn_manager = MagicMock()
    mock_conn_manager.provision.return_value = mock_conn
    mock_conn_manager_class.return_value = mock_conn_manager
    return mock_conn_manager


@pytest.fixture
def mock_settings_class():
    with patch("wealthz.runner.DuckLakeSettings") as mock:
        yield mock


@pytest.fixture
def mock_schema_syncer_class():
    with patch("wealthz.runner.DuckLakeSchemaSyncer") as mock:
        yield mock


@pytest.fixture
def mock_syncer(mock_schema_syncer_class):
    mock_syncer = MagicMock()
    mock_schema_syncer_class.return_value = mock_syncer
    return mock_syncer


@pytest.fixture
def mock_fetcher_factory_class():
    with patch("wealthz.runner.FetcherFactory") as mock:
        yield mock


@pytest.fixture
def mock_test_df():
    return pl.DataFrame({"col1": [1, 2, 3]})


@pytest.fixture
def mock_fetcher(mock_fetcher_factory_class, mock_test_df):
    mock_fetcher = MagicMock()
    mock_fetcher.fetch.return_value = mock_test_df
    return mock_fetcher


@pytest.fixture
def mock_fetcher_factory(mock_fetcher_factory_class, mock_fetcher):
    mock_fetcher_factory = MagicMock()
    mock_fetcher_factory.create.return_value = mock_fetcher
    mock_fetcher_factory_class.return_value = mock_fetcher_factory
    return mock_fetcher_factory


@pytest.fixture
def mock_transformed_df():
    return pl.DataFrame({"col1": ["1", "2", "3"]})


@pytest.fixture
def mock_transformer(mock_transformed_df):
    mock_transformer = MagicMock()
    mock_transformer.transform.return_value = mock_transformed_df
    return mock_transformer


@pytest.fixture
def mock_transformer_factory(mock_transformer):
    mock_transformer_factory = MagicMock()
    mock_transformer_factory.create.return_value = mock_transformer
    return mock_transformer_factory


@pytest.fixture
def mock_transformer_factory_class(mock_transformer_factory):
    with patch("wealthz.runner.TransformerFactory") as mock_transformer_factory_class:
        # Mock transformer factory and transformer
        mock_transformer_factory_class.return_value = mock_transformer_factory
        yield mock_transformer_factory_class


@pytest.fixture
def mock_loader():
    mock_loader = MagicMock()
    return mock_loader


@pytest.fixture
def mock_loader_factory(mock_loader):
    # Mock loader
    mock_loader_factory = MagicMock()
    mock_loader_factory.create.return_value = mock_loader
    return mock_loader_factory


@pytest.fixture
def mock_loader_factory_class(mock_loader_factory):
    with patch("wealthz.runner.LoaderFactory") as mock_loader_factory_class:
        mock_loader_factory_class.return_value = mock_loader_factory
        yield mock_loader_factory_class


def test_run_pipeline_success(
    mock_loader,
    mock_loader_factory,
    mock_loader_factory_class,
    mock_test_df,
    mock_transformed_df,
    mock_transformer,
    mock_transformer_factory,
    mock_transformer_factory_class,
    mock_fetcher,
    mock_fetcher_factory,
    mock_fetcher_factory_class,
    mock_syncer,
    mock_schema_syncer_class,
    mock_settings_class,
    mock_conn_manager,
    mock_conn,
    mock_pipeline,
):
    runner = PipelineRunner()

    # Act
    runner.run(mock_pipeline)

    # Assert
    mock_conn_manager.provision.assert_called_once()
    mock_syncer.sync.assert_called_once_with(mock_pipeline)
    mock_fetcher_factory_class.assert_called_once_with(mock_pipeline, mock_conn)
    mock_fetcher.fetch.assert_called_once()
    mock_transformer_factory_class.assert_called_once_with(mock_pipeline)
    mock_transformer.transform.assert_called_once_with(mock_test_df)
    mock_loader_factory_class.create.called_once()
    mock_loader.load.assert_called_once_with(mock_transformed_df, mock_pipeline)


def test_run_pipeline_handles_fetch_error(
    mock_loader,
    mock_loader_factory,
    mock_loader_factory_class,
    mock_transformed_df,
    mock_transformer,
    mock_transformer_factory,
    mock_transformer_factory_class,
    mock_fetcher,
    mock_fetcher_factory,
    mock_fetcher_factory_class,
    mock_syncer,
    mock_schema_syncer_class,
    mock_settings_class,
    mock_conn_manager,
    mock_conn,
    mock_pipeline,
):
    mock_fetcher.fetch.side_effect = RuntimeError("Fetch failed")

    runner = PipelineRunner()

    # Act & Assert
    with pytest.raises(RuntimeError, match="Fetch failed"):
        runner.run(mock_pipeline)


def test_run_pipeline_handles_transform_error(
    mock_loader,
    mock_loader_factory,
    mock_loader_factory_class,
    mock_transformer,
    mock_transformer_factory,
    mock_transformer_factory_class,
    mock_fetcher,
    mock_fetcher_factory,
    mock_fetcher_factory_class,
    mock_syncer,
    mock_schema_syncer_class,
    mock_settings_class,
    mock_conn_manager_class,
    mock_conn_manager,
    mock_conn,
    mock_pipeline,
):
    # Mock transformer to raise exception
    mock_transformer.transform.side_effect = ValueError("Transform failed")

    # Mock loader
    mock_loader = MagicMock()
    mock_loader_factory = MagicMock()
    mock_loader_factory.create.return_value = mock_loader
    mock_loader_factory_class.return_value = mock_loader_factory

    runner = PipelineRunner()

    # Act & Assert
    with pytest.raises(ValueError, match="Transform failed"):
        runner.run(mock_pipeline)


def test_run_pipeline_handles_fetch(mock_settings_class, mock_conn_manager_class, mock_conn_manager):
    # Arrange
    mock_settings = MagicMock()
    mock_settings_class.return_value = mock_settings

    # Act
    runner = PipelineRunner()

    # Assert
    assert runner.settings == mock_settings
    assert runner.conn_manager == mock_conn_manager
    mock_conn_manager_class.assert_called_once_with(mock_settings)

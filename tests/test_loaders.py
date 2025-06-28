import time
from copy import copy
from unittest.mock import MagicMock, patch

import polars as pl
import psycopg2
import pytest
from conftest import GSHEET_ETL_PIPELINE, PEOPLE_ETL_PIPELINE
from duckdb.duckdb import DuckDBPyConnection
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from wealthz.loaders import (
    DuckLakeConnManager,
    DuckLakeFullReplicationStrategy,
    DuckLakeIncrementalReplicationStrategy,
    DuckLakeLoader,
    DuckLakeSchemaSyncer,
    GoogleSheetsLoader,
    UnknownReplicationStrategy,
    query_build,
)
from wealthz.model import Column, ColumnType, ETLPipeline, GoogleSheetDestination, ReplicationType
from wealthz.settings import DuckLakeSettings, PostgresCatalogSettings, StorageSettings


def test_when_loading_into_ducklake_then_succeeds():
    mock_conn = MagicMock(spec=DuckDBPyConnection)
    loader = DuckLakeLoader(GSHEET_ETL_PIPELINE, mock_conn)
    df = pl.DataFrame({"id": [4, 5], "name": ["David", "Eve"]})
    loader.load(df)
    mock_conn.register.called_once_with("staging", df)
    mock_conn.execute.called_once_with("INSERT INTO public.people SELECT * FROM staging")


def wait_for_postgres(dsn, retries=10, delay=10):
    for _ in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return  # noqa: TRY300
        except psycopg2.OperationalError:
            time.sleep(delay)
    raise RuntimeError


@pytest.fixture()
def postgres_container():
    with PostgresContainer("postgres:15", driver=None) as postgres:
        dsn = postgres.get_connection_url()
        wait_for_postgres(dsn)
        print("Postgres container ready!")
        print(f"URL: {dsn}")
        print(f"DB: {postgres.dbname}")
        print(f"User: {postgres.username}")
        print(f"Host: {postgres.get_container_host_ip()}")
        print(f"Port: {postgres.get_exposed_port(5432)}")
        yield postgres


@pytest.mark.integration
@pytest.mark.parametrize("replication", list(ReplicationType))
def test_ducklake_with_minio_and_postgres(postgres_container, replication):
    # Start Postgres
    with MinioContainer() as minio:
        # Setup DuckDB connection
        minio_client = minio.get_client()
        bucket_name = "test-bucket"
        minio_client.make_bucket(bucket_name)

        storage_settings = StorageSettings(
            type="s3",
            endpoint=minio.get_config()["endpoint"],
            region="us-east-1",
            access_key_id=minio.access_key,
            secret_access_key=minio.secret_key,
            url_style="path",
            use_ssl=False,
            data_path=f"s3://{bucket_name}/data",
        )
        pg_config = PostgresCatalogSettings(
            dbname=postgres_container.dbname,
            host=postgres_container.get_container_host_ip(),
            port=postgres_container.get_exposed_port(5432),
            user=postgres_container.username,
            password=postgres_container.password,
        )
        settings = DuckLakeSettings(storage=storage_settings, pg=pg_config)
        manager = DuckLakeConnManager(settings)
        conn = manager.provision()
        syncer = DuckLakeSchemaSyncer(conn)
        syncer.sync(PEOPLE_ETL_PIPELINE)

        # Create table and insert test data
        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        # Register the dataframe as a DuckDB view
        pipeline = copy(PEOPLE_ETL_PIPELINE)
        pipeline.replication = replication
        assert pipeline.replication == replication
        DuckLakeLoader(GSHEET_ETL_PIPELINE, conn).load(df)

        # Query back the data
        result = conn.execute("SELECT * FROM people").pl()

        assert result.shape == (2, 2)

        assert result.sort("id")["name"].to_list() == ["Alice", "Bob"]


def test_query_build():
    template = "SELECT * FROM $table WHERE id = $id"
    result = query_build(template, table="users", id=123)
    assert result == "SELECT * FROM users WHERE id = 123"


def test_replication_strategy_pass():
    """Test abstract method implementation requirement"""
    from wealthz.loaders import ReplicationStrategy

    class TestStrategy(ReplicationStrategy):
        def replicate(self, pipeline):
            pass

    strategy = TestStrategy()
    strategy.replicate(PEOPLE_ETL_PIPELINE)


def test_duck_lake_schema_syncer_extract_duck_type():
    """Test type mapping in DuckLakeSchemaSyncer"""
    mock_conn = MagicMock()
    syncer = DuckLakeSchemaSyncer(mock_conn)

    from wealthz.model import Column

    string_col = Column(name="test", type="string")
    int_col = Column(name="test", type="integer")

    assert syncer.extract_duck_type(string_col) == "varchar"
    assert syncer.extract_duck_type(int_col) == "integer"


def test_duck_lake_base_replication_execute_delete_where():
    """Test delete operation in replication"""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = [5]

    strategy = DuckLakeIncrementalReplicationStrategy(mock_conn, "staging")
    strategy.execute_delete_where(PEOPLE_ETL_PIPELINE)

    mock_conn.execute.assert_called()


def test_duck_lake_base_replication_execute_truncate():
    """Test truncate operation in replication"""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = [10]

    strategy = DuckLakeFullReplicationStrategy(mock_conn, "staging")
    strategy.execute_truncate(PEOPLE_ETL_PIPELINE)

    mock_conn.execute.assert_called()


def test_duck_lake_incremental_replication_strategy():
    """Test incremental replication strategy"""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchone.return_value = [3]

    strategy = DuckLakeIncrementalReplicationStrategy(mock_conn, "staging")
    strategy.replicate(PEOPLE_ETL_PIPELINE)

    # Should call both delete and insert
    assert mock_conn.execute.call_count >= 2


def test_unknown_replication_strategy_exception():
    """Test UnknownReplicationStrategy exception"""
    invalid_replication = "invalid_type"
    exception = UnknownReplicationStrategy(invalid_replication)
    assert str(exception) == f"Unknown replication strategy: {invalid_replication}"


def test_duck_lake_loader_transaction_rollback():
    """Test transaction rollback on exception"""
    mock_conn = MagicMock()
    mock_conn.register.side_effect = Exception("Database error")

    loader = DuckLakeLoader(PEOPLE_ETL_PIPELINE, mock_conn)
    df = pl.DataFrame({"id": [1], "name": ["test"]})

    loader.load(df)

    mock_conn.begin.assert_called_once()
    mock_conn.rollback.assert_called_once()


def test_duck_lake_loader_unknown_replication_type():
    """Test unknown replication type handling"""
    mock_conn = MagicMock()

    # Create pipeline with invalid replication type
    invalid_pipeline = PEOPLE_ETL_PIPELINE.model_copy()
    invalid_pipeline.replication = "unknown"
    loader = DuckLakeLoader(invalid_pipeline, mock_conn)

    with pytest.raises(UnknownReplicationStrategy):
        loader.get_replication_strategy()


def test_duck_lake_conn_manager_configure_gcs_storage():
    """Test GCS storage configuration"""
    storage_settings = StorageSettings(
        type="gcs",
        access_key_id="test_key",
        secret_access_key="test_secret",  # noqa: S106
        data_path="gcs://test-bucket/data",
    )
    pg_settings = PostgresCatalogSettings(dbname="test", host="localhost", port=5432, user="user", password="pass")  # noqa: S106
    settings = DuckLakeSettings(storage=storage_settings, pg=pg_settings)
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = ["DuckDB v1.0"]

        manager = DuckLakeConnManager(settings)
        manager.configure_gcs_storage()

        mock_conn.execute.assert_called()


def test_duck_lake_conn_manager_configure_s3_storage():
    """Test S3 storage configuration"""
    storage_settings = StorageSettings(
        type="s3",
        region="us-east-1",
        access_key_id="test_key",
        secret_access_key="test_secret",  # noqa: S106
        endpoint="http://localhost:9000",
        data_path="s3://test-bucket/data",
    )
    pg_settings = PostgresCatalogSettings(dbname="test", host="localhost", port=5432, user="user", password="pass")  # noqa: S106
    settings = DuckLakeSettings(storage=storage_settings, pg=pg_settings)

    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = ["DuckDB v1.0"]

        manager = DuckLakeConnManager(settings)
        manager.configure_s3_storage()

        # Should call execute for each non-None setting
        assert mock_conn.execute.call_count >= 1


def test_duck_lake_conn_manager_unsupported_storage_type():
    """Test unsupported storage type"""
    storage_settings = StorageSettings(
        type="azure",  # Unsupported type
        access_key_id="test_key",
        secret_access_key="test_secret",  # noqa: S106
        data_path="azure://test",
    )
    pg_settings = PostgresCatalogSettings(dbname="test", host="localhost", port=5432, user="user", password="pass")  # noqa: S106
    settings = DuckLakeSettings(storage=storage_settings, pg=pg_settings)

    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.execute.return_value.fetchone.return_value = ["DuckDB v1.0"]

        manager = DuckLakeConnManager(settings)

        with pytest.raises(NotImplementedError):
            manager.configure_storage()


def test_replication_strategy_abstract_method():
    """Test that ReplicationStrategy is abstract"""
    from wealthz.loaders import ReplicationStrategy

    # This should raise TypeError since replicate is not implemented
    with pytest.raises(TypeError):
        ReplicationStrategy()


def test_google_sheets_loader_dataframe_conversion():
    """Test converting DataFrame to Google Sheets data format"""
    # Mock credentials and create loader
    mock_credentials = MagicMock()

    # Create test data
    df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30], "value": [100.5, None]})

    # Create test pipeline
    columns = [
        Column(name="name", type=ColumnType.STRING),
        Column(name="age", type=ColumnType.INTEGER),
        Column(name="value", type=ColumnType.DOUBLE),
    ]
    pipeline = ETLPipeline(
        name="test_table",
        destination=GoogleSheetDestination(
            credentials_file="credentials.json", sheet_id="Sheet1", sheet_range="Tab:!A:Z"
        ),
        columns=columns,
        replication=ReplicationType.FULL,
        primary_keys=["name"],
        datasource={"type": "ducklake", "query": "SELECT * FROM test"},
    )
    loader = GoogleSheetsLoader(pipeline, mock_credentials)

    # Test data conversion
    result = loader._dataframe_to_sheets_data(df)

    # Check headers
    assert result[0] == ["name", "age", "value"]

    # Check data rows
    assert result[1] == ["Alice", "25", "100.5"]
    assert result[2] == ["Bob", "30", ""]  # None should be converted to empty string


@patch("wealthz.loaders.build")
def test_google_sheets_loader_write_to_sheet_full_replication(mock_build):
    """Test writing to Google Sheets with full replication"""

    # Mock the Google Sheets service
    mock_service = MagicMock()
    mock_build.return_value = mock_service

    # Mock credentials
    mock_credentials = MagicMock()

    # Create test pipeline
    columns = [Column(name="name", type=ColumnType.STRING), Column(name="age", type=ColumnType.INTEGER)]
    pipeline = ETLPipeline(
        name="test_table",
        destination=GoogleSheetDestination(
            credentials_file="credentials.json", sheet_id="Sheet1", sheet_range="Tab:!A:Z"
        ),
        columns=columns,
        replication=ReplicationType.FULL,
        primary_keys=["name"],
        datasource={"type": "ducklake", "query": "SELECT * FROM test"},
    )
    loader = GoogleSheetsLoader(pipeline, mock_credentials)

    # Test data
    test_data = [["name", "age"], ["Alice", "25"]]

    # Mock successful update response
    mock_service.spreadsheets().values().update().execute.return_value = {"updatedCells": 2}

    # Test write operation
    loader._write_to_sheet(test_data)

    # Verify clear was called
    mock_service.spreadsheets().values().clear.assert_called_once()

    # Verify update was called
    mock_service.spreadsheets().values().update.assert_called()

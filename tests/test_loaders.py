import time
from copy import copy
from unittest.mock import MagicMock

import polars as pl
import psycopg2
import pytest
from duckdb.duckdb import DuckDBPyConnection
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from tests.conftest import GSHEET_ETL_PIPELINE
from wealthz.loaders import DuckLakeConnManager, DuckLakeLoader
from wealthz.model import ETLPipeline, ReplicationType
from wealthz.settings import PostgresCatalogSettings, StorageSettings


def test_when_loading_into_ducklake_then_succeeds():
    mock_conn = MagicMock(spec=DuckDBPyConnection)
    loader = DuckLakeLoader(mock_conn)
    df = pl.DataFrame({"id": [4, 5], "name": ["David", "Eve"]})
    loader.load(df, GSHEET_ETL_PIPELINE)
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


TEST_PEOPLE_PIPELINE = ETLPipeline(
    engine={"type": "duckdb"},
    schema="public",
    name="people",
    columns=[
        {"name": "id", "type": "integer"},
        {"name": "name", "type": "string"},
    ],
    datasource={
        "type": "gsheet",
        "sheet_id": "test-id",
        "sheet_range": "test-sheet-range",
        "credentials_file": "mock-creds.json",
    },
    primary_keys=["id"],
    replication="full",
)


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

        manager = DuckLakeConnManager(storage_settings, pg_config)
        conn = manager.provision()

        # Create table and insert test data
        df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        # Register the dataframe as a DuckDB view
        pipeline = copy(TEST_PEOPLE_PIPELINE)
        pipeline.replication = replication
        assert pipeline.replication == replication
        DuckLakeLoader(conn).load(df, pipeline)

        # Query back the data
        result = conn.execute("SELECT * FROM public.people").pl()

        assert result.shape == (2, 2)

        assert result.sort("id")["name"].to_list() == ["Alice", "Bob"]

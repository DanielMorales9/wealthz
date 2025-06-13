import abc
from abc import ABC
from string import Template
from typing import Any, ClassVar, Generic

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from polars import DataFrame

from wealthz.generics import T
from wealthz.logutils import get_logger
from wealthz.model import ETLPipeline

logger = get_logger(__name__)


class Loader(ABC, Generic[T]):
    @abc.abstractmethod
    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None: ...


class DuckLakeLoader(Loader):
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        self.conn.register("staging", df)
        query_template = Template("INSERT INTO $schema_name.$table_name SELECT * FROM staging")
        insert_query = query_template.substitute(table_name=pipeline.name, schema_name=pipeline.destination_schema)
        self.conn.execute(insert_query)


class DuckLakeConnManager:
    EXTENSIONS: ClassVar[list[str]] = ["ducklake", "postgres"]
    SCHEMAS: ClassVar[list[str]] = ["public"]

    def __init__(self, s3_config: dict[str, Any], pg_config: dict[str, Any], data_path: str) -> None:
        self._conn = duckdb.connect()
        version = self._conn.execute("SELECT version()").fetchone()
        if version:
            logger.info(f"DuckDB version: {version[0]}")

        self._s3_config = s3_config
        self._pg_config = pg_config
        self._data_path = data_path

    def install_and_load_extensions(self) -> None:
        for extension in self.EXTENSIONS:
            self._conn.execute(f"INSTALL {extension};")
            self._conn.execute(f"LOAD {extension};")

    def configure_s3_parameters(self) -> None:
        # Configure DuckDB to use S3-compatible API
        for key, value in self._s3_config.items():
            self._conn.execute(f"SET s3_{key}='{value}';")

    def attach_ducklake_house(self) -> None:
        # Attach DuckLake catalog
        connection_string = " ".join(f"{key}={value}" for key, value in self._pg_config.items())
        self._conn.execute(
            f"ATTACH 'ducklake:postgres:{connection_string}' AS ducklake (DATA_PATH '{self._data_path}');"
        )
        self._conn.execute("USE ducklake")

    def ensure_schemas_exists(self) -> None:
        for schema in self.SCHEMAS:
            self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def run(self) -> DuckDBPyConnection:  # type: ignore[no-any-unimported]
        self.install_and_load_extensions()
        self.configure_s3_parameters()
        self.attach_ducklake_house()
        self.ensure_schemas_exists()
        return self._conn

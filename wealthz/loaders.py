import abc
from abc import ABC
from string import Template
from typing import Any, ClassVar, Generic

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from polars import DataFrame
from pydantic.v1 import BaseSettings

from wealthz.generics import T
from wealthz.logutils import get_logger
from wealthz.model import ETLPipeline

logger = get_logger(__name__)


class Loader(ABC, Generic[T]):
    @abc.abstractmethod
    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None: ...


DUCKDB_TYPES_MAP = {"string": "varchar"}


class DuckLakeLoader(Loader):
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def ensure_table_exists(self, pipeline: ETLPipeline) -> None:
        columns_def = ",\n".join(
            f"\t{column.name} {DUCKDB_TYPES_MAP.get(column.type, column.type).upper()}" for column in pipeline.columns
        )
        create_stmt = """CREATE TABLE IF NOT EXISTS $schema_name.$table_name (
$columns_def
)"""
        query = self.build_query(
            create_stmt, table_name=pipeline.name, schema_name=pipeline.destination_schema, columns_def=columns_def
        )
        logger.info(query)
        self.conn.execute(query)

    def build_append_query(self, pipeline: ETLPipeline) -> str:
        columns = ",".join(column.name for column in pipeline.columns)
        query_template = "INSERT INTO $schema_name.$table_name ($columns) SELECT $columns FROM staging"
        return self.build_query(
            query_template, table_name=pipeline.name, schema_name=pipeline.destination_schema, columns=columns
        )

    @staticmethod
    def build_query(query: str, **kwargs: str) -> str:
        query_template = Template(query)
        return query_template.substitute(**kwargs)

    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        self.ensure_table_exists(pipeline)
        self.conn.register("staging", df)
        insert_query = self.build_append_query(pipeline)
        result = self.conn.execute(insert_query).fetchone()
        stats = result[0] if result else -1
        logger.info("Number of Inserted rows: %s", stats)


class StorageSettings(BaseSettings):
    type: str
    access_key_id: str
    secret_access_key: str
    endpoint: str | None = None
    region: str | None = None
    url_style: str | None = None
    use_ssl: bool | None = None

    class Config:
        env_prefix = "STORAGE_"
        case_sensitive = False


class DuckLakeConnManager:
    EXTENSIONS: ClassVar[list[str]] = ["ducklake", "postgres"]
    SCHEMAS: ClassVar[list[str]] = ["public"]

    def __init__(self, storage_settings: StorageSettings, pg_config: dict[str, Any], data_path: str) -> None:
        self._conn = duckdb.connect()
        version = self._conn.execute("SELECT version()").fetchone()
        if version:
            logger.info(f"DuckDB version: {version[0]}")

        self._storage_settings = storage_settings
        self._pg_config = pg_config
        self._data_path = data_path

    def install_and_load_extensions(self) -> None:
        for extension in self.EXTENSIONS:
            self._conn.execute(f"INSTALL {extension};")
            self._conn.execute(f"LOAD {extension};")

    def configure_storage(self) -> None:
        # Configure DuckDB to use remote storage
        match self._storage_settings.type:
            case "s3":
                self.configure_s3_storage()
            case "gcs":
                self.configure_gcs_storage()
            case _:
                raise NotImplementedError

    def configure_gcs_storage(self) -> None:
        self._conn.execute(
            f"""CREATE OR REPLACE SECRET gcs_secret (
                    TYPE gcs,
                    KEY_ID '{self._storage_settings.access_key_id}',
                    SECRET '{self._storage_settings.secret_access_key}'
                )""",
        )

    def configure_s3_storage(self) -> None:
        for key, value in self._storage_settings.dict().items():
            if not (key == "type" or value is None):
                self._conn.execute(f"SET s3_{key}='{value}';")

    def attach_ducklake_house(self) -> None:
        # Attach DuckLake catalog
        connection_string = " ".join(f"{key}={value}" for key, value in self._pg_config.items())
        attach_command = f"ATTACH 'ducklake:postgres:{connection_string}' AS ducklake (DATA_PATH '{self._data_path}');"
        self._conn.execute(attach_command)
        self._conn.execute("USE ducklake")

    def ensure_schemas_exists(self) -> None:
        for schema in self.SCHEMAS:
            self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    def provision(self) -> DuckDBPyConnection:  # type: ignore[no-any-unimported]
        self.install_and_load_extensions()
        self.configure_storage()
        self.attach_ducklake_house()
        self.ensure_schemas_exists()
        return self._conn

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


class QueryBuilder:
    @staticmethod
    def build(template: str, **kwargs: Any) -> str:
        query_template = Template(template)
        return query_template.substitute(**kwargs)


class DuckLakeLoader(Loader):
    CREATE_TABLE_TPL_STMT = """CREATE TABLE IF NOT EXISTS $schema_name.$table_name (
    $columns_def
);"""
    INSERT_TPL_STMT = "INSERT INTO $schema_name.$table_name ($columns) SELECT $columns FROM staging;"
    TRUNCATE_TPL_STMT = "TRUNCATE TABLE $schema_name.$table_name;"

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def ensure_table_exists(self, pipeline: ETLPipeline) -> None:
        columns_def = ",\n".join(
            f"\t{column.name} {DUCKDB_TYPES_MAP.get(column.type, column.type).upper()}" for column in pipeline.columns
        )

        query = QueryBuilder.build(
            self.CREATE_TABLE_TPL_STMT,
            table_name=pipeline.name,
            schema_name=pipeline.destination_schema,
            columns_def=columns_def,
        )
        logger.info(query)
        self.conn.execute(query)

    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        self.ensure_table_exists(pipeline)
        truncate_query = QueryBuilder.build(
            self.TRUNCATE_TPL_STMT, table_name=pipeline.name, schema_name=pipeline.destination_schema
        )
        insert_query = QueryBuilder.build(
            self.INSERT_TPL_STMT,
            table_name=pipeline.name,
            schema_name=pipeline.destination_schema,
            columns=",".join(pipeline.column_names),
        )
        try:
            self.conn.begin()
            result = self.conn.execute(truncate_query).fetchone()
            stats = result[0] if result else -1
            logger.info("Number of deleted rows: %s", stats)

            self.conn.register("staging", df)
            result = self.conn.execute(insert_query).fetchone()
            stats = result[0] if result else -1
            logger.info("Number of Inserted rows: %s", stats)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            logger.exception("Transaction failed")


class StorageSettings(BaseSettings):
    type: str
    access_key_id: str
    secret_access_key: str
    data_path: str
    endpoint: str | None = None
    region: str | None = None
    url_style: str | None = None
    use_ssl: bool | None = None

    class Config:
        env_prefix = "STORAGE_"
        case_sensitive = False


class PostgresCatalogSettings(BaseSettings):
    dbname: str
    host: str
    port: str
    user: str
    password: str

    class Config:
        env_prefix = "PG_"
        case_sensitive = False

    @property
    def connection(self) -> str:
        return " ".join(f"{key}={value}" for key, value in self.dict().items())


class DuckLakeConnManager:
    EXTENSIONS: ClassVar[list[str]] = ["ducklake", "postgres"]
    SCHEMAS: ClassVar[list[str]] = ["public"]

    CREATE_SECRET_TPL_STMT = """CREATE OR REPLACE SECRET gcs_secret (
    TYPE gcs,
    KEY_ID '$access_key_id',
    SECRET '$secret_access_key'
)"""  # noqa: S105

    ATTACH_TPL_STMT = "ATTACH 'ducklake:postgres:$connection' AS ducklake (DATA_PATH '$data_path');"
    CREATE_SCHEMA_TPL_STMT = "CREATE SCHEMA IF NOT EXISTS $schema"

    def __init__(self, storage_settings: StorageSettings, pg_catalog_settings: PostgresCatalogSettings) -> None:
        self._conn = duckdb.connect()
        version = self._conn.execute("SELECT version()").fetchone()
        if version:
            logger.info(f"DuckDB version: {version[0]}")

        self._storage_settings = storage_settings
        self._pg_catalog_settings = pg_catalog_settings

    def install_and_load_extensions(self) -> None:
        for extension in self.EXTENSIONS:
            extension_stmt = QueryBuilder.build("INSTALL $extension", extension=extension)
            load_stmt = QueryBuilder.build("LOAD $extension", extension=extension)
            self._conn.execute(extension_stmt)
            self._conn.execute(load_stmt)

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
        create_secret_stmt = QueryBuilder.build(
            self.CREATE_SECRET_TPL_STMT,
            access_key_id=self._storage_settings.access_key_id,
            secret_access_key=self._storage_settings.secret_access_key,
        )
        self._conn.execute(create_secret_stmt)

    def configure_s3_storage(self) -> None:
        for key, value in self._storage_settings.dict().items():
            if not (key in ("type", "data_path") or value is None):
                set_stmt = QueryBuilder.build("SET s3_$key='$value';", key=key, value=value)
                self._conn.execute(set_stmt)

    def attach_ducklake_house(self) -> None:
        # Attach DuckLake catalog
        attach_stmt = QueryBuilder.build(
            self.ATTACH_TPL_STMT,
            connection=self._pg_catalog_settings.connection,
            data_path=self._storage_settings.data_path,
        )
        self._conn.execute(attach_stmt)
        self._conn.execute("USE ducklake")

    def ensure_schemas_exists(self) -> None:
        for schema in self.SCHEMAS:
            create_schema_stmt = QueryBuilder.build(self.CREATE_SCHEMA_TPL_STMT, schema=schema)
            self._conn.execute(create_schema_stmt)

    def provision(self) -> DuckDBPyConnection:  # type: ignore[no-any-unimported]
        self.install_and_load_extensions()
        self.configure_storage()
        self.attach_ducklake_house()
        self.ensure_schemas_exists()
        return self._conn

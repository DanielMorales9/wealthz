import abc
from abc import ABC
from string import Template
from typing import Any, ClassVar, Generic

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from polars import DataFrame

from wealthz.generics import T
from wealthz.logutils import get_logger
from wealthz.model import ETLPipeline, ReplicationType
from wealthz.settings import PostgresCatalogSettings, StorageSettings

EXT_LOAD_TPL_STMT = "LOAD $extension"

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
    CREATE_TABLE_TPL_STMT = """CREATE TABLE IF NOT EXISTS $schema_name.$table_name ($columns_def);"""
    INSERT_TPL_STMT = "INSERT INTO $schema_name.$table_name ($columns) SELECT $columns FROM staging;"
    TRUNCATE_TPL_STMT = "TRUNCATE TABLE $schema_name.$table_name;"
    DELETE_TPL_STMT = """DELETE FROM $schema_name.$table_name
WHERE ($primary_keys) IN (SELECT $primary_keys FROM staging);"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def ensure_table_exists(self, pipeline: ETLPipeline) -> None:
        columns_def = ",".join(
            f"\n\t{column.name} {DUCKDB_TYPES_MAP.get(column.type, column.type).upper()}" for column in pipeline.columns
        )

        query = QueryBuilder.build(
            self.CREATE_TABLE_TPL_STMT,
            table_name=pipeline.name,
            schema_name=pipeline.destination_schema,
            columns_def=columns_def,
        )
        logger.info(query)
        result = self.conn.execute(query).fetchone()
        stats = result[0] if result else -1
        logger.info("Create table result: %s", stats)

    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        self.ensure_table_exists(pipeline)
        try:
            self.conn.begin()

            match pipeline.replication:
                case ReplicationType.FULL:
                    self.execute_full_snapshot(df, pipeline)
                case ReplicationType.APPEND:
                    self.execute_append(df, pipeline)
                case ReplicationType.INCREMENTAL:
                    self.execute_incremental(df, pipeline)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            logger.exception("Transaction failed")

    def execute_append(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        insert_stmt = QueryBuilder.build(
            self.INSERT_TPL_STMT,
            table_name=pipeline.name,
            schema_name=pipeline.destination_schema,
            columns=",".join(pipeline.column_names),
        )
        logger.info(insert_stmt)
        self.conn.register("staging", df)
        result = self.conn.execute(insert_stmt).fetchone()
        stats = result[0] if result else -1
        logger.info("Number of inserted rows: %s", stats)

    def execute_full_snapshot(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        self.execute_truncate(pipeline)
        self.execute_append(df, pipeline)

    def execute_truncate(self, pipeline: ETLPipeline) -> None:
        truncate_stmt = QueryBuilder.build(
            self.TRUNCATE_TPL_STMT, table_name=pipeline.name, schema_name=pipeline.destination_schema
        )
        logger.info(truncate_stmt)
        result = self.conn.execute(truncate_stmt).fetchone()
        stats = result[0] if result else -1
        logger.info("Number of deleted rows: %s", stats)

    def execute_incremental(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        self.conn.register("staging", df)

        delete_stmt = QueryBuilder.build(
            self.DELETE_TPL_STMT,
            table_name=pipeline.name,
            schema_name=pipeline.destination_schema,
            columns=", ".join(pipeline.column_names),
            primary_keys=", ".join(pipeline.primary_keys),
        )
        logger.info(delete_stmt)
        result = self.conn.execute(delete_stmt).fetchone()
        num_updates = result[0] if result else -1
        logger.info("Number of updated rows: %s", num_updates)

        insert_stmt = QueryBuilder.build(
            self.INSERT_TPL_STMT,
            table_name=pipeline.name,
            schema_name=pipeline.destination_schema,
            columns=", ".join(pipeline.column_names),
        )
        logger.info(insert_stmt)
        result = self.conn.execute(insert_stmt).fetchone()
        num_inserts = result[0] if result else -1
        logger.info("Number of inserted rows: %s", num_inserts - num_updates)


class DuckLakeConnManager:
    EXTENSIONS: ClassVar[list[str]] = ["ducklake", "postgres"]
    SCHEMAS: ClassVar[list[str]] = ["public"]

    EXT_INSTALL_TPL_STMT = "INSTALL $extension"
    EXT_LOAD_TPL_STMT = "LOAD $extension"
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
            ext_install_stmt = QueryBuilder.build(self.EXT_INSTALL_TPL_STMT, extension=extension)
            ext_load_stmt = QueryBuilder.build(self.EXT_LOAD_TPL_STMT, extension=extension)
            self._conn.execute(ext_install_stmt)
            self._conn.execute(ext_load_stmt)

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

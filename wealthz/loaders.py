import abc
from abc import ABC
from string import Template
from typing import Any, ClassVar, Generic

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from polars import DataFrame

from wealthz.generics import T
from wealthz.logutils import get_logger
from wealthz.model import BaseConfig, ETLPipeline, ReplicationType
from wealthz.settings import PostgresCatalogSettings, StorageSettings

logger = get_logger(__name__)


class Loader(ABC, Generic[T]):
    @abc.abstractmethod
    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None: ...


DUCKDB_TYPES_MAP = {"string": "varchar"}


def query_build(template: str, **kwargs: Any) -> str:
    query_template = Template(template)
    return query_template.substitute(**kwargs)


class DuckLakeSchemaSyncer:
    CREATE_TABLE_TPL_STMT = """CREATE TABLE IF NOT EXISTS $table_name ($columns_def);"""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def sync(self, pipeline: ETLPipeline) -> None:
        columns_def = ",".join(
            f"\n\t{column.name} {DUCKDB_TYPES_MAP.get(column.type, column.type).upper()}" for column in pipeline.columns
        )

        query = query_build(
            self.CREATE_TABLE_TPL_STMT,
            table_name=pipeline.name,
            columns_def=columns_def,
        )
        logger.info(query)
        result = self.conn.execute(query).fetchone()
        stats = result[0] if result else -1
        logger.info("Create table result: %s", stats)


class DuckLakeLoader(Loader):
    INSERT_TPL_STMT = "INSERT INTO $table_name ($columns) SELECT $columns FROM $staging_table_name;"
    TRUNCATE_TPL_STMT = "TRUNCATE TABLE $table_name;"
    DELETE_TPL_STMT = """DELETE FROM $table_name
WHERE ($primary_keys) IN (SELECT $primary_keys FROM $staging_table_name);"""

    STAGING_TABLE_NAME = "staging"

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        try:
            self.conn.begin()
            self.conn.register(self.STAGING_TABLE_NAME, df)

            match pipeline.replication:
                case ReplicationType.FULL:
                    self.execute_full_snapshot(pipeline)
                case ReplicationType.APPEND:
                    self.execute_append(pipeline)
                case ReplicationType.INCREMENTAL:
                    self.execute_incremental(pipeline)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            logger.exception("Transaction failed")

    def execute_append(self, pipeline: ETLPipeline) -> None:
        insert_stmt = query_build(
            self.INSERT_TPL_STMT,
            table_name=pipeline.name,
            columns=",".join(pipeline.column_names),
            staging_table_name=self.STAGING_TABLE_NAME,
        )
        logger.info(insert_stmt)
        result = self.conn.execute(insert_stmt).fetchone()
        stats = result[0] if result else -1
        logger.info("Number of inserted rows: %s", stats)

    def execute_full_snapshot(self, pipeline: ETLPipeline) -> None:
        self.execute_truncate(pipeline)
        self.execute_append(pipeline)

    def execute_truncate(self, pipeline: ETLPipeline) -> None:
        truncate_stmt = query_build(
            self.TRUNCATE_TPL_STMT,
            table_name=pipeline.name,
        )
        logger.info(truncate_stmt)
        result = self.conn.execute(truncate_stmt).fetchone()
        stats = result[0] if result else -1
        logger.info("Number of deleted rows: %s", stats)

    def execute_incremental(self, pipeline: ETLPipeline) -> None:
        self.execute_delete(pipeline)
        self.execute_append(pipeline)

    def execute_delete(self, pipeline: ETLPipeline) -> None:
        delete_stmt = query_build(
            self.DELETE_TPL_STMT,
            table_name=pipeline.name,
            columns=", ".join(pipeline.column_names),
            primary_keys=", ".join(pipeline.primary_keys),
            staging_table_name=self.STAGING_TABLE_NAME,
        )
        logger.info(delete_stmt)
        result = self.conn.execute(delete_stmt).fetchone()
        num_deletes = result[0] if result else -1
        logger.info("Number of deleted rows: %s", num_deletes)


class DuckDBExtension(BaseConfig):
    name: str
    source: str


class DuckLakeConnManager:
    EXTENSIONS: ClassVar[list[DuckDBExtension]] = [
        DuckDBExtension(name="ducklake", source="core_nightly"),
        DuckDBExtension(name="postgres", source="core"),
    ]

    EXT_INSTALL_TPL_STMT = "INSTALL $extension FROM $source"
    EXT_LOAD_TPL_STMT = "LOAD $extension"
    CREATE_SECRET_TPL_STMT = """CREATE OR REPLACE SECRET gcs_secret (
    TYPE gcs,
    KEY_ID '$access_key_id',
    SECRET '$secret_access_key'
)"""  # noqa: S105
    ATTACH_TPL_STMT = "ATTACH 'ducklake:postgres:$connection' AS ducklake (DATA_PATH '$data_path');"

    def __init__(self, storage_settings: StorageSettings, pg_catalog_settings: PostgresCatalogSettings) -> None:
        self._conn = duckdb.connect()
        version = self._conn.execute("SELECT version()").fetchone()
        if version:
            logger.info(f"DuckDB version: {version[0]}")

        self._storage_settings = storage_settings
        self._pg_catalog_settings = pg_catalog_settings

    def install_extensions(self) -> None:
        for ext in self.EXTENSIONS:
            ext_install_stmt = query_build(self.EXT_INSTALL_TPL_STMT, extension=ext.name, source=ext.source)
            ext_load_stmt = query_build(self.EXT_LOAD_TPL_STMT, extension=ext.name, source=ext.source)
            self._conn.execute(ext_install_stmt)
            self._conn.execute(ext_load_stmt)

    # noinspection PyUnreachableCode
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
        create_secret_stmt = query_build(
            self.CREATE_SECRET_TPL_STMT,
            access_key_id=self._storage_settings.access_key_id,
            secret_access_key=self._storage_settings.secret_access_key,
        )
        self._conn.execute(create_secret_stmt)

    def configure_s3_storage(self) -> None:
        for key, value in self._storage_settings.dict().items():
            if not (key in ("type", "data_path") or value is None):
                set_stmt = query_build("SET s3_$key='$value';", key=key, value=value)
                self._conn.execute(set_stmt)

    def setup_catalog(self) -> None:
        # Attach DuckLake catalog
        attach_stmt = query_build(
            self.ATTACH_TPL_STMT,
            connection=self._pg_catalog_settings.connection,
            data_path=self._storage_settings.data_path,
        )
        self._conn.execute(attach_stmt)
        self._conn.execute("USE ducklake")

    def provision(self) -> DuckDBPyConnection:  # type: ignore[no-any-unimported]
        self.install_extensions()
        self.configure_storage()
        self.setup_catalog()
        return self._conn

import abc
from abc import ABC
from io import TextIOWrapper
from string import Template
from types import TracebackType
from typing import Any, ClassVar, Generic, Optional, cast

import duckdb
from duckdb.duckdb import DuckDBPyConnection
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from polars import DataFrame

from wealthz.generics import T
from wealthz.logutils import get_logger
from wealthz.model import BaseConfig, Column, ETLPipeline, GoogleSheetDestination, ReplicationType
from wealthz.settings import DuckLakeSettings

logger = get_logger(__name__)


class Loader(ABC, Generic[T]):
    @abc.abstractmethod
    def load(self, df: DataFrame) -> None: ...


class ReplicationStrategy(ABC):
    @abc.abstractmethod
    def replicate(self) -> None:
        pass


def query_build(template: str, **kwargs: Any) -> str:
    query_template = Template(template)
    return query_template.substitute(**kwargs)


class DuckLakeSchemaSyncer:
    CREATE_TABLE_TPL_STMT = """CREATE TABLE IF NOT EXISTS $table_name ($columns_def);"""
    DUCKDB_TYPES_MAP: ClassVar[dict[str, str]] = {"string": "varchar"}

    def __init__(self, pipeline: ETLPipeline, conn: duckdb.DuckDBPyConnection):
        self._pipeline = pipeline
        self.conn = conn

    def extract_duck_type(self, column: Column) -> str:
        return self.DUCKDB_TYPES_MAP.get(column.type, column.type)

    def sync(self) -> None:
        duck_columns = ((column.name, self.extract_duck_type(column)) for column in self._pipeline.columns)
        columns_def = ",".join(f"\n\t{name} {duck_type.upper()}" for name, duck_type in duck_columns)

        query = query_build(
            self.CREATE_TABLE_TPL_STMT,
            table_name=self._pipeline.name,
            columns_def=columns_def,
        )
        logger.info(query)
        result = self.conn.execute(query).fetchone()
        stats = result[0] if result else -1
        logger.info("Create table result: %s", stats)


class DuckLakeBaseReplicationStrategy(ReplicationStrategy, ABC):
    INSERT_TPL_STMT = "INSERT INTO $table_name ($columns) SELECT $columns FROM $staging_table_name;"
    TRUNCATE_TPL_STMT = "TRUNCATE TABLE $table_name;"
    DELETE_TPL_STMT = (
        """DELETE FROM $table_name WHERE ($primary_keys) IN (SELECT $primary_keys FROM $staging_table_name);"""
    )

    def __init__(self, pipeline: ETLPipeline, conn: duckdb.DuckDBPyConnection, staging_table_name: str):
        self._pipeline = pipeline
        self.conn = conn
        self.staging_table_name = staging_table_name

    def execute_insert_into(self) -> None:
        insert_stmt = query_build(
            self.INSERT_TPL_STMT,
            table_name=self._pipeline.name,
            columns=", ".join(self._pipeline.column_names),
            staging_table_name=self.staging_table_name,
        )
        logger.info(insert_stmt)
        result = self.conn.execute(insert_stmt).fetchone()
        stats = result[0] if result else -1
        logger.info("Number of inserted rows: %s", stats)

    def execute_truncate(self) -> None:
        truncate_stmt = query_build(
            self.TRUNCATE_TPL_STMT,
            table_name=self._pipeline.name,
        )
        logger.info(truncate_stmt)
        result = self.conn.execute(truncate_stmt).fetchone()
        stats = result[0] if result else -1
        logger.info("Number of deleted rows: %s", stats)

    def execute_delete_where(self) -> None:
        delete_stmt = query_build(
            self.DELETE_TPL_STMT,
            table_name=self._pipeline.name,
            columns=", ".join(self._pipeline.column_names),
            primary_keys=", ".join(self._pipeline.primary_keys),
            staging_table_name=self.staging_table_name,
        )
        logger.info(delete_stmt)
        result = self.conn.execute(delete_stmt).fetchone()
        num_deletes = result[0] if result else -1
        logger.info("Number of deleted rows: %s", num_deletes)


class DuckLakeAppendReplicationStrategy(DuckLakeBaseReplicationStrategy):
    def replicate(self) -> None:
        self.execute_insert_into()


class DuckLakeFullReplicationStrategy(DuckLakeBaseReplicationStrategy):
    def replicate(self) -> None:
        self.execute_truncate()
        self.execute_insert_into()


class DuckLakeIncrementalReplicationStrategy(DuckLakeBaseReplicationStrategy):
    def replicate(self) -> None:
        self.execute_delete_where()
        self.execute_insert_into()


class UnknownReplicationStrategy(ValueError):
    def __init__(self, replication_type: ReplicationType):
        super().__init__(f"Unknown replication strategy: {replication_type}")


class DuckLakeLoader(Loader):
    STAGING_TABLE_NAME = "staging"
    REPLICATION_STRATEGY_MAP: ClassVar[dict[ReplicationType, type[DuckLakeBaseReplicationStrategy]]] = {
        ReplicationType.FULL: DuckLakeFullReplicationStrategy,
        ReplicationType.APPEND: DuckLakeAppendReplicationStrategy,
        ReplicationType.INCREMENTAL: DuckLakeIncrementalReplicationStrategy,
    }

    def __init__(self, pipeline: ETLPipeline, conn: duckdb.DuckDBPyConnection):
        self._pipeline = pipeline
        self.conn = conn

    def load(self, df: DataFrame) -> None:
        try:
            self.conn.begin()
            self.conn.register(self.STAGING_TABLE_NAME, df)
            strategy = self.get_replication_strategy()
            strategy.replicate()
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            logger.exception("Transaction failed")

    def get_replication_strategy(self) -> DuckLakeBaseReplicationStrategy:
        strategy_class = self.REPLICATION_STRATEGY_MAP.get(self._pipeline.replication)
        if strategy_class is None:
            raise UnknownReplicationStrategy(self._pipeline.replication)
        return strategy_class(self._pipeline, self.conn, self.STAGING_TABLE_NAME)


class DuckDBExtension(BaseConfig):
    name: str
    source: str


class DuckLakeConnManager:
    EXTENSIONS: ClassVar[list[DuckDBExtension]] = [
        DuckDBExtension(name="ducklake", source="core_nightly"),
        DuckDBExtension(name="postgres", source="core"),
    ]

    EXT_INSTALL_TPL_STMT = "INSTALL $extension FROM $source;"
    EXT_LOAD_TPL_STMT = "LOAD $extension;"
    CREATE_SECRET_TPL_STMT = """CREATE OR REPLACE SECRET gcs_secret (
    TYPE gcs,
    KEY_ID '$access_key_id',
    SECRET '$secret_access_key'
);"""  # noqa: S105
    ATTACH_TPL_STMT = """ATTACH 'ducklake:postgres:$connection'
    AS $name (DATA_PATH '$data_path');"""
    USE_LAKE_TPL_STMT = "USE $name;"

    def __init__(
        self,
        settings: DuckLakeSettings,
    ) -> None:
        self.settings = settings
        self._setup_file: TextIOWrapper | None = None
        self._conn = duckdb.connect()
        self._log_version()

    def _log_version(self) -> None:
        version = self._conn.execute("SELECT version()").fetchone()
        if version:
            logger.info(f"DuckDB version: {version[0]}")

    def install_extensions(self) -> None:
        for ext in self.EXTENSIONS:
            ext_install_stmt = query_build(self.EXT_INSTALL_TPL_STMT, extension=ext.name, source=ext.source)
            ext_load_stmt = query_build(self.EXT_LOAD_TPL_STMT, extension=ext.name, source=ext.source)

            self._execute(ext_install_stmt, ext_load_stmt)

    # noinspection PyUnreachableCode
    def configure_storage(self) -> None:
        # Configure DuckDB to use remote storage
        STORAGE_SETUP_MAP = {
            "s3": self.configure_s3_storage,
            "gcs": self.configure_gcs_storage,
        }
        configure_fun = STORAGE_SETUP_MAP.get(self.settings.storage.type)
        if configure_fun is None:
            raise NotImplementedError
        configure_fun()

    def configure_gcs_storage(self) -> None:
        create_secret_stmt = query_build(
            self.CREATE_SECRET_TPL_STMT,
            access_key_id=self.settings.storage.access_key_id,
            secret_access_key=self.settings.storage.secret_access_key,
        )
        self._execute(create_secret_stmt)

    def configure_s3_storage(self) -> None:
        statements = []
        for key, value in self.settings.storage.dict().items():
            if not (key in ("type", "data_path") or value is None):
                set_stmt = query_build("SET s3_$key='$value';", key=key, value=value)
                statements.append(set_stmt)
        self._execute(*statements)

    def configure_catalog(self) -> None:
        # Attach DuckLake catalog
        attach_stmt = query_build(
            self.ATTACH_TPL_STMT,
            connection=self.settings.pg.connection,
            data_path=self.settings.storage.data_path,
            name=self.settings.name,
        )
        use_stmt = query_build(self.USE_LAKE_TPL_STMT, name=self.settings.name)
        self._execute(attach_stmt, use_stmt)

    def __enter__(self) -> "DuckLakeConnManager":
        if self.settings.setup_path:
            self._setup_file = open(self.settings.setup_path, "w")
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_inst: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if self._setup_file:
            self._setup_file.close()

        return not exc_type

    def _execute(self, *statements: str) -> None:
        if self._setup_file:
            for stmt in statements:
                self._setup_file.write(f"{stmt}\n")
            self._setup_file.write("\n")

        for stmt in statements:
            self._conn.execute(stmt)

    def provision(self) -> DuckDBPyConnection:  # type: ignore[no-any-unimported]
        with self:
            self.install_extensions()
            self.configure_storage()
            self.configure_catalog()
            return self._conn


class GoogleSheetsLoader(Loader):
    def __init__(self, pipeline: ETLPipeline, credentials: Credentials):
        destination = cast(GoogleSheetDestination, pipeline.destination)
        self._pipeline = pipeline
        self.sheet_id = destination.sheet_id
        self.sheet_range = destination.sheet_range
        self.credentials = credentials
        self.service = build("sheets", "v4", credentials=credentials)

    def load(self, df: DataFrame) -> None:
        try:
            data = self._dataframe_to_sheets_data(df)
            self._write_to_sheet(data)
            logger.info("Successfully loaded %d rows to Google Sheet", len(data) - 1)
        except Exception:
            logger.exception("Failed to load data to Google Sheet")
            raise

    def _dataframe_to_sheets_data(self, df: DataFrame) -> list[list[Any]]:
        data = [self._pipeline.column_names]

        for row in df.rows():
            formatted_row = []
            for value in row:
                if value is None:
                    formatted_row.append("")
                else:
                    formatted_row.append(str(value))
            data.append(formatted_row)

        return data

    def _write_to_sheet(self, data: list[list[Any]]) -> None:
        range_name = self.sheet_range or f"{self._pipeline.name}!A:Z"
        self._clear_sheet(range_name)

        body = {"values": data}

        result = (
            self.service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )

        logger.info("Google Sheets API response: %s", result.get("updatedCells", 0))

    def _clear_sheet(self, range_name: str) -> None:
        self.service.spreadsheets().values().clear(spreadsheetId=self.sheet_id, range=range_name, body={}).execute()
        logger.info("Cleared sheet range: %s", range_name)

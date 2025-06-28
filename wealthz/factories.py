from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Generic, cast

import duckdb
from google.oauth2.service_account import Credentials

from wealthz.constants import SECRETS_DIR
from wealthz.fetchers import DuckLakeFetcher, Fetcher, GoogleSheetFetcher, YFinanceFetcher
from wealthz.generics import T
from wealthz.loaders import DuckLakeLoader, GoogleSheetsLoader, Loader
from wealthz.model import DatasourceType, DestinationType, ETLPipeline, GoogleSheetDatasource
from wealthz.transforms import ColumnTransformer, NoopTransformer, Transformer

SPREADSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEETS_WRITE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class Factory(ABC, Generic[T]):
    @abstractmethod
    def create(self) -> T:
        pass


class GoogleCredentialsScope(StrEnum):
    SPREADSHEETS = "spreadsheets"
    SPREADSHEETS_WRITE = "spreadsheets_write"


class GoogleCredentialsFactory(Factory[Credentials]):
    def __init__(self, credential_file_name: str, scope: GoogleCredentialsScope | None = None):
        self._credentials_path = SECRETS_DIR / credential_file_name
        self._scopes = []
        if scope == GoogleCredentialsScope.SPREADSHEETS:
            self._scopes = SPREADSHEETS_SCOPES
        elif scope == GoogleCredentialsScope.SPREADSHEETS_WRITE:
            self._scopes = SPREADSHEETS_WRITE_SCOPES

    def create(self) -> Credentials:
        return Credentials.from_service_account_file(self._credentials_path, scopes=self._scopes)  # type: ignore[no-any-return]


class UnknownDataSourceTypeError(ValueError):
    def __init__(self, source_type: DatasourceType):
        super().__init__(f"Unknown datasource type: {source_type}")


class UnknownDestinationTypeError(ValueError):
    def __init__(self, destination_type: DestinationType):
        super().__init__(f"Unknown destination type: {destination_type}")


class LoaderFactory(Factory[Loader]):
    """Factory for creating appropriate loader based on destination type."""

    def __init__(self, pipeline: ETLPipeline, conn: duckdb.DuckDBPyConnection) -> None:
        self._pipeline = pipeline
        self._conn = conn

    def create(self) -> Loader:
        """Create loader based on destination type."""
        if self._pipeline.destination is None:
            return DuckLakeLoader(self._pipeline, self._conn)
        elif self._pipeline.destination.type == DestinationType.GOOGLE_SHEET:
            creds_manager = GoogleCredentialsFactory(
                self._pipeline.destination.credentials_file, scope=GoogleCredentialsScope.SPREADSHEETS_WRITE
            )
            credentials = creds_manager.create()
            return GoogleSheetsLoader(self._pipeline, credentials)
        elif self._pipeline.destination.type == DestinationType.DUCKLAKE:
            return DuckLakeLoader(self._pipeline, self._conn)
        else:
            raise UnknownDestinationTypeError(self._pipeline.destination.type)


class FetcherFactory(Factory[Fetcher]):
    """Factory for creating appropriate fetcher based on datasource type."""

    def __init__(self, pipeline: ETLPipeline, conn: duckdb.DuckDBPyConnection) -> None:
        self._pipeline = pipeline
        self._conn = conn

    def create_gsheet_datasource(self) -> GoogleSheetFetcher:
        gsheet_datasource = cast(GoogleSheetDatasource, self._pipeline.datasource)
        creds_factory = GoogleCredentialsFactory(
            gsheet_datasource.credentials_file, scope=GoogleCredentialsScope.SPREADSHEETS
        )
        credentials = creds_factory.create()
        return GoogleSheetFetcher(self._pipeline, credentials)

    def create(self) -> Fetcher:
        """Create fetcher based on datasource type."""
        datasource_type = self._pipeline.datasource.type
        if datasource_type == DatasourceType.GOOGLE_SHEET:
            return self.create_gsheet_datasource()
        elif datasource_type == DatasourceType.DUCKLAKE:
            return DuckLakeFetcher(self._pipeline, self._conn)
        elif datasource_type == DatasourceType.YFINANCE:
            return YFinanceFetcher(self._pipeline)
        raise UnknownDataSourceTypeError(datasource_type)


class TransformerFactory(Factory[Transformer]):
    def __init__(self, pipeline: ETLPipeline) -> None:
        self._pipeline = pipeline

    def create(self) -> Transformer:
        if self._pipeline.has_transforms:
            return ColumnTransformer(self._pipeline.columns)
        return NoopTransformer()

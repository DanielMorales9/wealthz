from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Generic, cast

import duckdb
from google.oauth2.service_account import Credentials

from wealthz.constants import SECRETS_DIR
from wealthz.fetchers import DuckLakeFetcher, Fetcher, GoogleSheetFetcher, YFinanceFetcher
from wealthz.generics import T
from wealthz.model import DatasourceType, ETLPipeline, GoogleSheetDatasource

SPREADSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class Factory(ABC, Generic[T]):
    @abstractmethod
    def create(self) -> T:
        pass


class GoogleCredentialsScope(StrEnum):
    SPREADSHEETS = "spreadsheets"


class GoogleCredentialsFactory(Factory[Credentials]):
    def __init__(self, credential_file_name: str, scope: GoogleCredentialsScope | None = None):
        self._credentials_path = SECRETS_DIR / credential_file_name
        self._scopes = []
        if scope == GoogleCredentialsScope.SPREADSHEETS:
            self._scopes = SPREADSHEETS_SCOPES

    def create(self) -> Credentials:
        return Credentials.from_service_account_file(self._credentials_path, scopes=self._scopes)  # type: ignore[no-any-return]


class UnknownDataSourceTypeError(ValueError):
    def __init__(self, source_type: DatasourceType):
        super().__init__(f"Unknown datasource type: {source_type}")


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
        return GoogleSheetFetcher(credentials)

    def create(self) -> Fetcher:
        """Create fetcher based on datasource type."""
        datasource_type = self._pipeline.datasource.type
        if datasource_type == DatasourceType.GOOGLE_SHEET:
            return self.create_gsheet_datasource()
        elif datasource_type == DatasourceType.DUCKLAKE:
            return DuckLakeFetcher(self._conn)
        elif datasource_type == DatasourceType.YFINANCE:
            return YFinanceFetcher()
        else:
            raise UnknownDataSourceTypeError(datasource_type)

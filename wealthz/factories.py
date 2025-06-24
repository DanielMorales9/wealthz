from abc import ABC, abstractmethod
from enum import StrEnum
from pathlib import Path
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
    def __init__(self, path: Path, scope: GoogleCredentialsScope | None = None):
        self._credentials_path = path
        self._scopes = []
        if scope == GoogleCredentialsScope.SPREADSHEETS:
            self._scopes = SPREADSHEETS_SCOPES

    def create(self) -> Credentials:
        return Credentials.from_service_account_file(self._credentials_path, scopes=self._scopes)  # type: ignore[no-any-return]


class GoogleSheetFetcherFactory(Factory[GoogleSheetFetcher]):
    def __init__(self, pipeline: ETLPipeline) -> None:
        datasource = cast(GoogleSheetDatasource, pipeline.datasource)
        self._credentials_path = SECRETS_DIR / datasource.credentials_file

    def create(self) -> GoogleSheetFetcher:
        creds_manager = GoogleCredentialsFactory(self._credentials_path, scope=GoogleCredentialsScope.SPREADSHEETS)
        credentials = creds_manager.create()
        return GoogleSheetFetcher(credentials)


class UnknownDataSourceTypeError(ValueError):
    def __init__(self, source_type: DatasourceType):
        super().__init__(f"Unknown datasource type: {source_type}")


class FetcherFactory:
    """Factory for creating appropriate fetcher based on datasource type."""

    @staticmethod
    def create_fetcher(pipeline: ETLPipeline, conn: duckdb.DuckDBPyConnection) -> Fetcher:
        """Create fetcher based on datasource type."""
        if pipeline.datasource.type == DatasourceType.GOOGLE_SHEET:
            return GoogleSheetFetcherFactory(pipeline).create()
        elif pipeline.datasource.type == DatasourceType.DUCKLAKE:
            return DuckLakeFetcher(conn)
        elif pipeline.datasource.type == DatasourceType.YFINANCE:
            return YFinanceFetcher()
        else:
            raise UnknownDataSourceTypeError(pipeline.datasource.type)

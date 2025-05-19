from abc import ABC, abstractmethod
from enum import StrEnum
from pathlib import Path
from typing import Generic

from google.oauth2.service_account import Credentials

from wealthz.constants import GOOGLE_CREDENTIALS_FILENAME, SECRETS_PATH
from wealthz.fetchers import GoogleSheetFetcher
from wealthz.generics import T

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
    def __init__(self, sheet_id: str, sheet_range: str) -> None:
        self._sheet_id = sheet_id
        self._sheet_range = sheet_range
        self._credentials_path = SECRETS_PATH / GOOGLE_CREDENTIALS_FILENAME

    def create(self) -> GoogleSheetFetcher:
        creds_manager = GoogleCredentialsFactory(self._credentials_path, scope=GoogleCredentialsScope.SPREADSHEETS)
        credentials = creds_manager.create()
        return GoogleSheetFetcher(self._sheet_id, self._sheet_range, credentials)

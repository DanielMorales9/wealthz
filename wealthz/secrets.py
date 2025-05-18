import abc
from abc import ABC
from enum import StrEnum
from pathlib import Path
from typing import Generic

from google.oauth2.service_account import Credentials

from wealthz.generics import T

SPREADSHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


class SecretsManager(ABC, Generic[T]):
    @abc.abstractmethod
    def create(self) -> T: ...


class GoogleCredentialsScope(StrEnum):
    SPREADSHEETS = "spreadsheets"


class GoogleCredentialsManager(SecretsManager[Credentials]):
    def __init__(self, path: Path, scope: GoogleCredentialsScope | None = None):
        self._credentials_path = path
        self._scopes = []
        if scope == GoogleCredentialsScope.SPREADSHEETS:
            self._scopes = SPREADSHEETS_SCOPES

    def create(self) -> Credentials:
        return Credentials.from_service_account_file(self._credentials_path, scopes=self._scopes)  # type: ignore[no-any-return]

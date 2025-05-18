import abc
from abc import ABC

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build  # type: ignore[import-untyped]
from polars import DataFrame


class Fetcher(ABC):
    @abc.abstractmethod
    def fetch(self) -> DataFrame: ...


class GoogleSheetFetcher(Fetcher):
    def __init__(self, sheet_id: str, sheet_range: str, credentials: Credentials) -> None:
        # Build Sheets API client
        service = build("sheets", "v4", credentials=credentials)
        self._sheet_client = service.spreadsheets()
        self._sheet_id = sheet_id
        self._sheet_range = sheet_range

    def fetch(self) -> DataFrame:
        # Fetch the sheet data
        result = self._sheet_client.values().get(spreadsheetId=self._sheet_id, range=self._sheet_range).execute()
        values = result.get("values", [])

        if not values:
            print("No data found in the sheet.")
            return DataFrame()

        # First row as header
        headers = values[0]
        data_rows = values[1:]

        # Create Polars DataFrame
        df = DataFrame(data_rows, schema=headers, orient="row")

        return df

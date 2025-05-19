import abc
from abc import ABC

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build  # type: ignore[import-untyped]
from polars import DataFrame

from wealthz.model import ETLPipeline


class Fetcher(ABC):
    @abc.abstractmethod
    def fetch(self, pipeline: ETLPipeline) -> DataFrame: ...


class GoogleSheetFetcher(Fetcher):
    def __init__(self, credentials: Credentials) -> None:
        # Build Sheets API client
        service = build("sheets", "v4", credentials=credentials)
        self._sheet_client = service.spreadsheets()

    def fetch(self, pipeline: ETLPipeline) -> DataFrame:
        # Fetch the sheet data
        datasource = pipeline.datasource
        sheet_values = self._sheet_client.values()
        result = sheet_values.get(spreadsheetId=datasource.sheet_id, range=datasource.sheet_range).execute()
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

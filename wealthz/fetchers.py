import abc
from abc import ABC

import polars
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build  # type: ignore[import-untyped]
from polars import DataFrame

from wealthz.model import ETLPipeline


class Fetcher(ABC):
    @abc.abstractmethod
    def fetch(self, pipeline: ETLPipeline) -> DataFrame: ...


class GoogleSheetFetcher(Fetcher):
    def __init__(self, credentials: Credentials) -> None:
        service = build("sheets", "v4", credentials=credentials)
        self._sheet_client = service.spreadsheets()

    def fetch(self, pipeline: ETLPipeline) -> DataFrame:
        # Fetch the sheet data
        datasource = pipeline.datasource
        sheet_values = self._sheet_client.values()
        result = sheet_values.get(spreadsheetId=datasource.sheet_id, range=datasource.sheet_range).execute()
        values = result.get("values", [])
        schema = {col.name: polars.String for col in pipeline.columns}
        if not values:
            print("No data found in the sheet.")
            return DataFrame(schema=schema)

        # First row as header
        rows = values[1:]

        # Create Polars DataFrame with all columns as strings
        df = DataFrame(rows, schema=schema, orient="row", infer_schema_length=0)

        return df

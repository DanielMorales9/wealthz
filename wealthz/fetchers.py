import abc
from abc import ABC

import polars
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build  # type: ignore[import-untyped]
from polars import DataFrame

from wealthz.model import ColumnType, ETLPipeline


class Fetcher(ABC):
    @abc.abstractmethod
    def fetch(self) -> DataFrame: ...


POLARS_SCHEMA_OVERRIDE = {
    ColumnType.STRING: polars.String,
    ColumnType.INTEGER: polars.Int32,
    ColumnType.FLOAT: polars.Float32,
    ColumnType.BOOLEAN: polars.Boolean,
    ColumnType.DATE: polars.Date,
    ColumnType.TIMESTAMP: polars.Datetime,
}


class GoogleSheetFetcher(Fetcher):
    def __init__(self, pipeline: ETLPipeline, credentials: Credentials) -> None:
        self._pipeline = pipeline
        service = build("sheets", "v4", credentials=credentials)
        self._sheet_client = service.spreadsheets()

    def fetch(self) -> DataFrame:
        # Fetch the sheet data
        datasource = self._pipeline.datasource
        sheet_values = self._sheet_client.values()
        result = sheet_values.get(spreadsheetId=datasource.sheet_id, range=datasource.sheet_range).execute()
        values = result.get("values", [])

        if not values:
            print("No data found in the sheet.")
            return DataFrame()

        # First row as header
        rows = values[1:]

        # Create Polars DataFrame with all columns as strings
        schema = {col.name: polars.String for col in self._pipeline.columns}
        df = DataFrame(rows, schema=schema, orient="row", infer_schema_length=0)

        return df

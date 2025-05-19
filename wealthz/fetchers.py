import abc
from abc import ABC

import polars
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build  # type: ignore[import-untyped]
from polars import DataFrame

from wealthz.model import ColumnType, ETLPipeline


class Fetcher(ABC):
    @abc.abstractmethod
    def fetch(self, pipeline: ETLPipeline) -> DataFrame: ...


POLARS_SCHEMA_OVERRIDE = {
    ColumnType.STRING: polars.String,
    ColumnType.INTEGER: polars.Int32,
    ColumnType.FLOAT: polars.Float32,
    ColumnType.BOOLEAN: polars.Boolean,
    ColumnType.DATE: polars.Date,
    ColumnType.TIMESTAMP: polars.Datetime,
}


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
        rows = values[1:]

        # Create Polars DataFrame
        schema = {col.name: POLARS_SCHEMA_OVERRIDE[col.type] for col in pipeline.columns}
        df = DataFrame(rows, schema=schema, orient="row", infer_schema_length=0)

        return df

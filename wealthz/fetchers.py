import abc
import logging
from abc import ABC
from typing import cast

import duckdb
import polars
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build  # type: ignore[import-untyped]
from polars import DataFrame

from wealthz.model import DuckLakeDatasource, ETLPipeline, GoogleSheetDatasource

logger = logging.getLogger(__name__)


class Fetcher(ABC):
    @abc.abstractmethod
    def fetch(self, pipeline: ETLPipeline) -> DataFrame: ...


class GoogleSheetFetcher(Fetcher):
    def __init__(self, credentials: Credentials) -> None:
        service = build("sheets", "v4", credentials=credentials)
        self._sheet_client = service.spreadsheets()

    def fetch(self, pipeline: ETLPipeline) -> DataFrame:
        # Fetch the sheet data
        logger.info("Fetching data from Google Sheet")
        datasource = cast(GoogleSheetDatasource, pipeline.datasource)
        sheet_values = self._sheet_client.values()
        result = sheet_values.get(spreadsheetId=datasource.sheet_id, range=datasource.sheet_range).execute()
        values = result.get("values", [])
        schema = {col.name: polars.String for col in pipeline.columns}
        if not values:
            logger.info("No data found in the sheet.")
            return DataFrame(schema=schema)

        # First row as header
        rows = values[1:]

        # Create Polars DataFrame with all columns as strings
        df = DataFrame(rows, schema=schema, orient="row", infer_schema_length=0)

        return df


class DuckLakeFetcher(Fetcher):
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def fetch(self, pipeline: ETLPipeline) -> DataFrame:
        logging.info("Fetching data from DuckDB...")
        datasource = cast(DuckLakeDatasource, pipeline.datasource)
        logger.info("Query: %s", datasource.query)
        return self.conn.execute(datasource.query).pl()

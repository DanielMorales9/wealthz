import abc
import logging
from abc import ABC
from typing import cast

import duckdb
import pandas as pd
import polars as pl
import yfinance as yf
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from polars import DataFrame

from wealthz.constants import DATETIME_ISO_FORMAT
from wealthz.logutils import get_logger
from wealthz.model import DuckLakeDatasource, ETLPipeline, GoogleSheetDatasource, YFinanceDatasource

logger = get_logger(__name__)


class Fetcher(ABC):
    @abc.abstractmethod
    def fetch(self) -> DataFrame: ...


class GoogleSheetFetcher(Fetcher):
    def __init__(self, pipeline: ETLPipeline, credentials: Credentials) -> None:
        self._pipeline = pipeline
        service = build("sheets", "v4", credentials=credentials)
        self._sheet_client = service.spreadsheets()

    def fetch(self) -> DataFrame:
        # Fetch the sheet data
        logger.info("Fetching data from Google Sheet")
        datasource = cast(GoogleSheetDatasource, self._pipeline.datasource)
        sheet_values = self._sheet_client.values()
        result = sheet_values.get(spreadsheetId=datasource.sheet_id, range=datasource.sheet_range).execute()
        values = result.get("values", [])
        schema = {col.name: pl.String for col in self._pipeline.columns}
        if not values:
            logger.info("No data found in the sheet.")
            return DataFrame(schema=schema)

        # First row as header
        rows = values[1:]

        # Create Polars DataFrame with all columns as strings
        df = DataFrame(rows, schema=schema, orient="row", infer_schema_length=0)

        return df


class DuckLakeFetcher(Fetcher):
    def __init__(self, pipeline: ETLPipeline, conn: duckdb.DuckDBPyConnection):
        self._pipeline = pipeline
        self.conn = conn

    def fetch(self) -> DataFrame:
        logging.info("Fetching data from DuckDB...")
        datasource = cast(DuckLakeDatasource, self._pipeline.datasource)
        logger.info("Query: %s", datasource.query)
        return self.conn.execute(datasource.query).pl()


class YFinanceFetcher(Fetcher):
    SYMBOL_COLUMN = "Symbol"

    def __init__(self, pipeline: ETLPipeline) -> None:
        self._pipeline = pipeline

    def fetch(self) -> DataFrame:
        logger.info("Fetching Yahoo Finance data...")
        datasource = cast(YFinanceDatasource, self._pipeline.datasource)

        dataframes = []
        for symbol in datasource.symbols:
            logger.info("Fetching history for symbol %s", symbol)
            dat = yf.Ticker(symbol)
            df = dat.history(period=datasource.period, interval=datasource.interval, raise_errors=True)
            # extracting datetime from index
            df.index = df.index.tz_convert("UTC").strftime(DATETIME_ISO_FORMAT)
            df = df.reset_index()
            logger.debug("Columns for symbol %s: %s", symbol, ", ".join(df.columns))
            df[self.SYMBOL_COLUMN] = symbol
            dataframes.append(df)

        df = pd.concat(dataframes, ignore_index=True)
        return pl.from_pandas(df)

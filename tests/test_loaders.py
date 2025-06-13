from unittest.mock import MagicMock

import duckdb.duckdb
import polars as pl

from tests.conftest import GSHEET_ETL_PIPELINE
from wealthz.loaders import DuckLakeLoader


def test_when_loading_into_ducklake_then_succeeds():
    mock_conn = MagicMock(spec=duckdb.duckdb.DuckDBPyConnection)
    loader = DuckLakeLoader(mock_conn)
    df = pl.DataFrame({"id": [4, 5], "name": ["David", "Eve"]})
    loader.load(df, GSHEET_ETL_PIPELINE)
    mock_conn.execute.called_once_with("INSERT INTO ducklake.public.people SELECT * FROM df")

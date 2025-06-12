import os
import shutil
from pathlib import Path
from string import Template

import duckdb
import pytest
from polars import DataFrame

from wealthz.constants import DUCKDB_LOCAL_META_PATH
from wealthz.loaders import DuckDBLoader
from wealthz.model import Column, ColumnType, ETLPipeline, GoogleSheetDatasource


@pytest.fixture(autouse=True)
def dwh_dir():
    metadata = Path(DUCKDB_LOCAL_META_PATH)
    metadata.parent.mkdir(parents=True, exist_ok=True)
    yield metadata.parent
    shutil.rmtree(os.path.dirname(DUCKDB_LOCAL_META_PATH))


@pytest.mark.parametrize(
    "pipeline, data, expected",
    [
        (
            ETLPipeline(
                engine={"type": "duckdb"},
                schema="public",
                name="test_pipeline",
                datasource=GoogleSheetDatasource(
                    sheet_id="test_sheet_id", sheet_range="A1:B2", credentials_file="mock_creds.json"
                ),
                columns=[
                    Column(name="column1", type=ColumnType.INTEGER),
                    Column(name="column2", type=ColumnType.STRING),
                ],
            ),
            {"column1": [1, 2], "column2": ["a", "b"]},
            [(1, "a"), (2, "b")],
        )
    ],
)
def test_duckdb_loader(dwh_dir, pipeline, data, expected):
    df = DataFrame(data)
    conn = duckdb.connect(DUCKDB_LOCAL_META_PATH)

    # Create a DuckDBLoader instance
    loader = DuckDBLoader(pipeline)

    # Call the load method
    loader.load(df)

    # Check if the database file was created
    query_template = Template("SELECT * from $schema.$name")
    query = query_template.substitute(schema=pipeline.schema_, name=pipeline.name)
    assert conn.execute(query).fetchall() == [(1, "a"), (2, "b")]

from string import Template

import duckdb
import pytest
from polars import DataFrame

from wealthz.loaders import DuckDBLoader
from wealthz.model import Column, ColumnType, ETLPipeline, GoogleSheetDatasource


@pytest.mark.parametrize(
    "pipeline, data, expected",
    [
        (
            ETLPipeline(
                engine={"type": "duckdb", "storage": "fs"},
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
def test_duckdb_loader(tmp_path, pipeline, data, expected):
    df = DataFrame(data)
    meta_file = tmp_path / "meta.duckdb"
    data_path = tmp_path / "data"
    data_path.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(meta_file)

    # Create a DuckDBLoader instance
    loader = DuckDBLoader(db_path=meta_file, base_path=data_path)

    # Call the load method
    loader.load(df, pipeline)

    # Check if the database file was created
    query_template = Template("SELECT * from $schema.$name")
    query = query_template.substitute(schema=pipeline.schema_, name=pipeline.name)
    assert conn.execute(query).fetchall() == [(1, "a"), (2, "b")]

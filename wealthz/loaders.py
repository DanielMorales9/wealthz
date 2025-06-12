import abc
import os
from abc import ABC
from pathlib import Path
from string import Template
from typing import Generic

import duckdb
from polars import DataFrame

from wealthz.constants import DUCKDB_LOCAL_DATA_PATH, DUCKDB_LOCAL_META_PATH
from wealthz.generics import T
from wealthz.model import ETLPipeline


class Loader(ABC, Generic[T]):
    @abc.abstractmethod
    def load(self, df: DataFrame) -> None: ...


class DuckDBLoader(Loader):
    def __init__(self, pipeline: ETLPipeline) -> None:
        self._pipeline = pipeline
        self._conn = duckdb.connect(DUCKDB_LOCAL_META_PATH)
        self._base_path = DUCKDB_LOCAL_DATA_PATH

    def load(self, df: DataFrame) -> None:
        # Register the in-memory DataFrame as a DuckDB relation
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._pipeline.schema_}")
        self._conn.register("df", df.to_arrow())

        # Write to Parquet
        table_path = Path(self._base_path) / self._pipeline.schema_ / self._pipeline.name / "data.parquet"
        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        copy_template = Template("COPY df TO '$path' (FORMAT PARQUET);")
        copy_stmt = copy_template.substitute(path=str(table_path))
        self._conn.execute(copy_stmt)

        # Create external table
        create_template = Template("CREATE OR REPLACE TABLE $schema.$table AS SELECT * FROM '$path';")
        create_stmt = create_template.substitute(
            path=table_path, schema=self._pipeline.schema_, table=self._pipeline.name
        )
        self._conn.execute(create_stmt)
        self._conn.close()

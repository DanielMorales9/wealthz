import abc
import os
from abc import ABC
from pathlib import Path
from string import Template
from typing import Generic

import duckdb
from polars import DataFrame

from wealthz.generics import T
from wealthz.model import ETLPipeline


class Loader(ABC, Generic[T]):
    @abc.abstractmethod
    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None: ...


class DuckDBLoader(Loader):
    def __init__(self, db_path: Path, base_path: Path) -> None:
        self._conn = duckdb.connect(db_path)
        self._base_path = base_path

    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        # Register the in-memory DataFrame as a DuckDB relation
        self._conn.register("df", df.to_arrow())

        # Write to Parquet
        table_path = self._base_path / pipeline.schema_ / pipeline.name / "data.parquet"
        os.makedirs(os.path.dirname(table_path), exist_ok=True)

        copy_template = Template("COPY df TO '$path' (FORMAT PARQUET);")
        copy_stmt = copy_template.substitute(path=str(table_path))
        print(copy_stmt)
        self._conn.execute(copy_stmt)

        # Create external table
        create_template = Template("CREATE OR REPLACE TABLE $schema.$table AS SELECT * FROM '$path';")
        create_stmt = create_template.substitute(path=table_path, schema=pipeline.schema_, table=pipeline.name)
        self._conn.execute(create_stmt)
        self._conn.close()

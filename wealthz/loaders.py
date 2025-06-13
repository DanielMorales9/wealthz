import abc
from abc import ABC
from string import Template
from typing import Generic

import duckdb
from polars import DataFrame

from wealthz.generics import T
from wealthz.model import ETLPipeline


class Loader(ABC, Generic[T]):
    @abc.abstractmethod
    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None: ...


class DuckLakeLoader(Loader):
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def load(self, df: DataFrame, pipeline: ETLPipeline) -> None:
        query_template = Template("INSERT INTO ducklake.$schema_name.$table_name SELECT * FROM df")
        insert_query = query_template.substitute(table_name=pipeline.name, schema_name=pipeline.schema_)
        self.conn.execute(
            insert_query,
            {
                "df": df,
            },
        )

from enum import StrEnum
from typing import Union

from pydantic import BaseModel, ConfigDict, Field


class BaseConfig(BaseModel):
    model_config = ConfigDict(use_enum_values=True, extra="forbid")


class ColumnType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"


class Column(BaseConfig):
    name: str
    type: ColumnType


class Table(BaseConfig):
    schema_: str = Field(..., alias="schema")
    name: str
    columns: list[Column]


class GoogleSheetDatasource(BaseModel):
    id: str
    range: str


Datasource = Union[GoogleSheetDatasource]


class ETLPipeline(Table):
    datasource: Datasource

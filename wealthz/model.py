from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

from wealthz.utils import load_yaml


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


class DatasourceType(StrEnum):
    GOOGLE_SHEET = "gsheet"


class GoogleSheetDatasource(BaseModel):
    type: Literal[DatasourceType.GOOGLE_SHEET] = DatasourceType.GOOGLE_SHEET
    sheet_id: str
    sheet_range: str
    credentials_file: str


Datasource = Annotated[Union[GoogleSheetDatasource], Field(discriminator="type")]


class EngineType(StrEnum):
    DUCKDB = "duckdb"


class StorageType(StrEnum):
    LOCAL = "local"
    GCS = "gcs"


class Engine(BaseConfig):
    type: EngineType
    storage: StorageType


class ETLPipeline(Table):
    engine: Engine
    datasource: Datasource

    @classmethod
    def from_yaml(cls, file_path: Path) -> "ETLPipeline":
        obj = load_yaml(file_path)
        return ETLPipeline.model_validate(obj)

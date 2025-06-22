from enum import StrEnum
from functools import cached_property
from pathlib import Path
from typing import Annotated, Literal, Optional, Union

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


class TransformType(StrEnum):
    CAST = "cast"
    TRIM = "trim"
    UPPER = "upper"
    LOWER = "lower"
    REGEX_REPLACE = "regex_replace"
    SPLIT = "split"
    SUBSTRING = "substring"
    DATE_FORMAT = "date_format"


class BaseParams(BaseModel):
    pass


class CastParams(BaseParams):
    target_type: ColumnType = Field(..., description="Target type for casting")


class RegexReplaceParams(BaseParams):
    pattern: str = Field(..., description="Regular expression pattern")
    replacement: str = Field(default="", description="Replacement string")


class SplitParams(BaseParams):
    delimiter: str = Field(..., description="Delimiter to split on")
    index: int = Field(default=0, description="Index of part to extract")


class SubstringParams(BaseParams):
    start: int = Field(default=0, description="Start position")
    length: Optional[int] = Field(default=None, description="Length of substring")


class DateFormatParams(BaseParams):
    input_format: str = Field(..., description="Input date format")


ColumnTransformParams = Union[
    BaseParams,
    CastParams,
    RegexReplaceParams,
    SplitParams,
    SubstringParams,
    DateFormatParams,
]


class Transform(BaseConfig):
    type: TransformType
    params: ColumnTransformParams = Field(default_factory=BaseParams)


class Column(BaseConfig):
    name: str
    type: ColumnType
    transforms: list[Transform] = Field(default_factory=list)


class ReplicationType(StrEnum):
    APPEND = "append"
    FULL = "full"
    INCREMENTAL = "incremental"


class Table(BaseConfig):
    name: str
    columns: list[Column]
    replication: ReplicationType
    primary_keys: list[str]

    @cached_property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]


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


class Engine(BaseConfig):
    type: EngineType


class ETLPipeline(Table):
    engine: Engine
    datasource: Datasource

    @classmethod
    def from_yaml(cls, file_path: Path) -> "ETLPipeline":
        obj = load_yaml(file_path)
        return ETLPipeline.model_validate(obj)

    @cached_property
    def has_transforms(self) -> bool:
        return any(column.transforms for column in self.columns)

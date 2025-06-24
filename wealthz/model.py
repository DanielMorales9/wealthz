from enum import StrEnum
from functools import cached_property
from pathlib import Path
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from wealthz.utils import load_yaml

YAHOO_FINANCE_VALID_INTERVAL = (
    "1m",
    "2m",
    "5m",
    "15m",
    "30m",
    "60m",
    "90m",
    "1h",
    "4h",
    "1d",
    "5d",
    "1wk",
    "1mo",
    "3mo",
)


class BaseConfig(BaseModel):
    model_config = ConfigDict(use_enum_values=True, extra="forbid")


class ColumnType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
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


class CastTransform(BaseModel):
    type: Literal[TransformType.CAST] = TransformType.CAST
    target_type: ColumnType = Field(..., description="Target type for casting")


class TrimTransform(BaseModel):
    type: Literal[TransformType.TRIM] = TransformType.TRIM


class UpperTransform(BaseModel):
    type: Literal[TransformType.UPPER] = TransformType.UPPER


class LowerTransform(BaseModel):
    type: Literal[TransformType.LOWER] = TransformType.LOWER


class RegexReplaceTransform(BaseModel):
    type: Literal[TransformType.REGEX_REPLACE] = TransformType.REGEX_REPLACE
    pattern: str = Field(..., description="Regular expression pattern")
    replacement: str = Field(default="", description="Replacement string")


class SplitTransform(BaseModel):
    type: Literal[TransformType.SPLIT] = TransformType.SPLIT
    delimiter: str = Field(..., description="Delimiter to split on")
    index: int = Field(default=0, description="Index of part to extract")


class SubstringTransform(BaseModel):
    type: Literal[TransformType.SUBSTRING] = TransformType.SUBSTRING
    start: int = Field(default=0, description="Start position")
    length: Optional[int] = Field(default=None, description="Length of substring")


class DateFormatTransform(BaseModel):
    type: Literal[TransformType.DATE_FORMAT] = TransformType.DATE_FORMAT
    input_format: str = Field(..., description="Input date format")


Transform = Annotated[
    Union[
        CastTransform,
        TrimTransform,
        UpperTransform,
        LowerTransform,
        RegexReplaceTransform,
        SplitTransform,
        SubstringTransform,
        DateFormatTransform,
    ],
    Field(discriminator="type"),
]


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
    DUCKLAKE = "ducklake"
    YFINANCE = "yfinance"


class GoogleSheetDatasource(BaseModel):
    type: Literal[DatasourceType.GOOGLE_SHEET] = DatasourceType.GOOGLE_SHEET
    sheet_id: str
    sheet_range: str
    credentials_file: str


class DuckLakeDatasource(BaseModel):
    type: Literal[DatasourceType.DUCKLAKE] = DatasourceType.DUCKLAKE
    query: str = Field(..., description="Query string")


class YFinanceDatasource(BaseModel):
    type: Literal[DatasourceType.YFINANCE] = DatasourceType.YFINANCE
    symbols: list[str] = Field(..., description="Stock ticker symbol (e.g., AAPL)")
    period: str = Field(..., description="Stock ticker period")
    interval: str = Field(..., description="Stock ticker interval")

    @field_validator("interval")
    def validate_interval(cls, interval: str) -> str:
        if interval not in YAHOO_FINANCE_VALID_INTERVAL:
            raise ValueError(f"Invalid interval '{interval}'")
        return interval


Datasource = Annotated[
    Union[GoogleSheetDatasource, DuckLakeDatasource, YFinanceDatasource], Field(discriminator="type")
]


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

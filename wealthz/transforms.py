"""Column-level transform engine for ETL pipeline."""

from abc import ABC, abstractmethod
from typing import ClassVar, cast

import polars as pl
from polars import DataType

from wealthz.model import (
    BaseParams,
    CastParams,
    Column,
    ColumnType,
    DateFormatParams,
    RegexReplaceParams,
    SplitParams,
    SubstringParams,
    TransformType,
)


class ColumnTransform(ABC):
    """Abstract base class for all transforms."""

    @abstractmethod
    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        """Apply the transform to a Polars expression."""
        pass


class CastTransform(ColumnTransform):
    """Cast column to specified type."""

    TYPE_MAPPING: ClassVar[dict[ColumnType, type[DataType]]] = {
        ColumnType.STRING: pl.Utf8,
        ColumnType.INTEGER: pl.Int64,
        ColumnType.FLOAT: pl.Float64,
        ColumnType.BOOLEAN: pl.Boolean,
        ColumnType.DATE: pl.Date,
        ColumnType.TIMESTAMP: pl.Datetime,
    }

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        params = cast(CastParams, params)
        _type: type[DataType] = self.TYPE_MAPPING[params.target_type]
        return expr.cast(_type)


class TrimTransform(ColumnTransform):
    """Trim whitespace from string columns."""

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        return expr.str.strip_chars()


class UpperTransform(ColumnTransform):
    """Convert string to uppercase."""

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        return expr.str.to_uppercase()


class LowerTransform(ColumnTransform):
    """Convert string to lowercase."""

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        return expr.str.to_lowercase()


class RegexReplaceTransform(ColumnTransform):
    """Replace using regular expressions."""

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        params = cast(RegexReplaceParams, params)
        return expr.str.replace_all(params.pattern, params.replacement, literal=False)


class SplitTransform(ColumnTransform):
    """Split string and extract specific part."""

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        params = cast(SplitParams, params)
        return expr.str.split(params.delimiter).list.get(params.index, null_on_oob=True)


class SubstringTransform(ColumnTransform):
    """Extract substring."""

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        params = cast(SubstringParams, params)
        if params.length is not None:
            return expr.str.slice(params.start, params.length)
        else:
            return expr.str.slice(params.start)


class DateFormatTransform(ColumnTransform):
    """Parse date strings with custom format."""

    def apply(self, expr: pl.Expr, params: BaseParams) -> pl.Expr:
        params = cast(DateFormatParams, params)
        # Use strict=False to handle parsing errors gracefully
        parsed = expr.str.to_datetime(params.input_format)
        return parsed


class UnknownColumnTransformError(ValueError):
    def __init__(self, transform_type: str) -> None:
        super().__init__(f"Unknown transform type: {transform_type}")


class ColumnTransformEngine:
    """Engine for applying column-level transforms to DataFrames."""

    COLUMN_TRANSFORM_MAP: ClassVar[dict[str, ColumnTransform]] = {
        TransformType.CAST: CastTransform(),
        TransformType.TRIM: TrimTransform(),
        TransformType.UPPER: UpperTransform(),
        TransformType.LOWER: LowerTransform(),
        TransformType.REGEX_REPLACE: RegexReplaceTransform(),
        TransformType.SPLIT: SplitTransform(),
        TransformType.SUBSTRING: SubstringTransform(),
        TransformType.DATE_FORMAT: DateFormatTransform(),
    }

    def apply(self, df: pl.DataFrame, columns: list[Column]) -> pl.DataFrame:
        """Apply all column-level transforms to a DataFrame."""
        if not columns:
            return df

        # Build column expressions with transforms
        expressions = []
        for column in columns:
            expr = self.build_column_expression(column)
            expressions.append(expr)

        return df.select(expressions)

    def build_column_expression(self, column: Column) -> pl.Expr:
        expr = pl.col(column.name)
        for transform in column.transforms:
            transform_impl = self.get_column_transform(transform.type)
            expr = transform_impl.apply(expr, transform.params)
        return expr.alias(column.name)

    def get_column_transform(self, transform: TransformType) -> ColumnTransform:
        if not (transform_impl := self.COLUMN_TRANSFORM_MAP.get(transform)):
            raise UnknownColumnTransformError(transform)
        return transform_impl

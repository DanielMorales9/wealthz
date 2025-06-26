"""Column-level transform engine for ETL pipeline."""

import abc
from abc import ABC, abstractmethod
from typing import ClassVar, cast

import polars as pl
from polars import DataType

from wealthz.model import (
    CastTransform,
    Column,
    ColumnType,
    DateFormatTransform,
    RegexReplaceTransform,
    SplitTransform,
    SubstringTransform,
    Transform,
    TransformType,
)


class ColumnTransform(ABC):
    """Abstract base class for all transforms."""

    @abstractmethod
    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        """Apply the transform to a Polars expression."""
        pass


class CastColumnTransform(ColumnTransform):
    """Cast column to specified type."""

    TYPE_MAPPING: ClassVar[dict[ColumnType, type[DataType]]] = {
        ColumnType.STRING: pl.Utf8,
        ColumnType.INTEGER: pl.Int32,
        ColumnType.LONG: pl.Int64,
        ColumnType.FLOAT: pl.Float32,
        ColumnType.DOUBLE: pl.Float64,
        ColumnType.BOOLEAN: pl.Boolean,
        ColumnType.DATE: pl.Date,
        ColumnType.TIMESTAMP: pl.Datetime,
    }

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        transform = cast(CastTransform, transform)
        _type: type[DataType] = self.TYPE_MAPPING[transform.target_type]
        return expr.cast(_type)


class TrimColumnTransform(ColumnTransform):
    """Trim whitespace from string columns."""

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        return expr.str.strip_chars()


class UpperColumnTransform(ColumnTransform):
    """Convert string to uppercase."""

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        return expr.str.to_uppercase()


class LowerColumnTransform(ColumnTransform):
    """Convert string to lowercase."""

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        return expr.str.to_lowercase()


class RegexReplaceColumnTransform(ColumnTransform):
    """Replace using regular expressions."""

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        transform = cast(RegexReplaceTransform, transform)
        return expr.str.replace_all(transform.pattern, transform.replacement, literal=False)


class SplitColumnTransform(ColumnTransform):
    """Split string and extract specific part."""

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        transform = cast(SplitTransform, transform)
        return expr.str.split(transform.delimiter).list.get(transform.index, null_on_oob=True)


class SubstringColumnTransform(ColumnTransform):
    """Extract substring."""

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        transform = cast(SubstringTransform, transform)
        if transform.length is not None:
            return expr.str.slice(transform.start, transform.length)
        else:
            return expr.str.slice(transform.start)


class DateFormatColumnTransform(ColumnTransform):
    """Parse date strings with custom format."""

    def apply(self, expr: pl.Expr, transform: Transform) -> pl.Expr:
        transform = cast(DateFormatTransform, transform)
        # Use strict=False to handle parsing errors gracefully
        parsed = expr.str.to_datetime(transform.input_format)
        return parsed


class UnknownColumnTransformError(ValueError):
    def __init__(self, transform_type: str) -> None:
        super().__init__(f"Unknown transform type: {transform_type}")


class Transformer(ABC):
    @abc.abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        pass


class NoopTransformer(Transformer):
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        return df


class ColumnTransformer(Transformer):
    """Engine for applying column-level transforms to DataFrames."""

    COLUMN_TRANSFORM_MAP: ClassVar[dict[str, ColumnTransform]] = {
        TransformType.CAST: CastColumnTransform(),
        TransformType.TRIM: TrimColumnTransform(),
        TransformType.UPPER: UpperColumnTransform(),
        TransformType.LOWER: LowerColumnTransform(),
        TransformType.REGEX_REPLACE: RegexReplaceColumnTransform(),
        TransformType.SPLIT: SplitColumnTransform(),
        TransformType.SUBSTRING: SubstringColumnTransform(),
        TransformType.DATE_FORMAT: DateFormatColumnTransform(),
    }

    def __init__(self, columns: list[Column]) -> None:
        self._columns = columns

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply all column-level transforms to a DataFrame."""
        if not self._columns:
            return df

        # Build column expressions with transforms
        expressions = []
        for column in self._columns:
            expr = self.build_column_expression(column)
            expressions.append(expr)

        return df.select(expressions)

    def build_column_expression(self, column: Column) -> pl.Expr:
        expr = pl.col(column.name)
        for transform in column.transforms:
            transform_impl = self.get_column_transform(transform.type)
            expr = transform_impl.apply(expr, transform)
        return expr.alias(column.name)

    def get_column_transform(self, transform: TransformType) -> ColumnTransform:
        if not (transform_impl := self.COLUMN_TRANSFORM_MAP.get(transform)):
            raise UnknownColumnTransformError(transform)
        return transform_impl

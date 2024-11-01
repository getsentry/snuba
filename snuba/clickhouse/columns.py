from __future__ import annotations

from typing import Sequence, Union

from snuba.utils.schemas import UUID, AggregateFunction, Any, Array, Column
from snuba.utils.schemas import ColumnSet as BaseColumnSet
from snuba.utils.schemas import (
    ColumnType,
    Date,
    DateTime,
    DateTime64,
    Enum,
    FixedString,
    FlattenedColumn,
    Float,
    Int,
    IPv4,
    IPv6,
    Map,
    Nested,
    Nullable,
    ReadOnly,
    SchemaModifiers,
    SimpleAggregateFunction,
    String,
    TModifiers,
    Tuple,
    TypeModifier,
    TypeModifiers,
    UInt,
    WildcardColumn,
)

__all__ = (
    "Any",
    "AggregateFunction",
    "Array",
    "Column",
    "ColumnSet",
    "ColumnType",
    "Date",
    "DateTime",
    "DateTime64",
    "Enum",
    "FixedString",
    "FlattenedColumn",
    "Float",
    "IPv4",
    "IPv6",
    "Map",
    "Nested",
    "Nullable",
    "ReadOnly",
    "SchemaModifiers",
    "SimpleAggregateFunction",
    "String",
    "TModifiers",
    "TypeModifier",
    "TypeModifiers",
    "Tuple",
    "UInt",
    "Int",
    "UUID",
)


class ColumnSet(BaseColumnSet):
    """\
    A set of columns, unique by column name.
    Initialized with a list of Column objects or
    (column_name: String, column_type: ColumnType) tuples.
    Offers simple functionality:
    * ColumnSets can be added together (order is maintained)
    * Columns can be looked up by ClickHouse normalized names, e.g. 'tags.key'
    * `for_schema()` can be used to generate valid ClickHouse column names
      and types for a table schema.
    """

    def __init__(
        self,
        columns: Sequence[
            Union[Column[SchemaModifiers], tuple[str, ColumnType[SchemaModifiers]]]
        ],
    ) -> None:
        for column in columns:
            assert not isinstance(column, WildcardColumn)

        super().__init__(Column.to_columns(columns))

    def __repr__(self) -> str:
        return "ColumnSet({})".format(repr(self.columns))

    def __add__(
        self,
        other: Union[ColumnSet, Sequence[tuple[str, ColumnType[SchemaModifiers]]]],
    ) -> ColumnSet:
        if isinstance(other, ColumnSet):
            return ColumnSet([*self.columns, *other.columns])
        return ColumnSet([*self.columns, *other])

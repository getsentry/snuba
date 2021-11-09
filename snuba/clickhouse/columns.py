from __future__ import annotations

from typing import Sequence, Tuple, Union, cast

from snuba.utils.schemas import UUID, AggregateFunction, Any, Array, Column
from snuba.utils.schemas import ColumnSet as BaseColumnSet
from snuba.utils.schemas import (
    ColumnType,
    Date,
    DateTime,
    Enum,
    FixedString,
    FlattenedColumn,
    Float,
    IPv4,
    IPv6,
    Nested,
    Nullable,
    ReadOnly,
    SchemaModifiers,
    String,
    TModifiers,
    TypeModifier,
    TypeModifiers,
    UInt,
)

__all__ = (
    "ColumnSet",
    "Array",
    "Any",
    "AggregateFunction",
    "Column",
    "ColumnType",
    "Date",
    "DateTime",
    "Enum",
    "FixedString",
    "FlattenedColumn",
    "Float",
    "IPv4",
    "IPv6",
    "Nullable",
    "Nested",
    "ReadOnly",
    "SchemaModifiers",
    "String",
    "TModifiers",
    "TypeModifier",
    "TypeModifiers",
    "UInt",
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
            Union[Column[SchemaModifiers], Tuple[str, ColumnType[SchemaModifiers]]]
        ],
    ) -> None:
        super().__init__(Column.to_columns(columns))

    def __repr__(self) -> str:
        return "ColumnSet({})".format(repr(self.columns))

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self._flattened == cast(ColumnSet, other)._flattened
        )

    def __len__(self) -> int:
        return len(self._flattened)

    def __add__(
        self,
        other: Union[ColumnSet, Sequence[Tuple[str, ColumnType[SchemaModifiers]]]],
    ) -> ColumnSet:
        if isinstance(other, ColumnSet):
            return ColumnSet([*self.columns, *other.columns])
        return ColumnSet([*self.columns, *other])

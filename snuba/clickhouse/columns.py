from __future__ import annotations

from typing import (
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

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
    "Any",
    "AggregateFunction",
    "Array",
    "Column",
    "ColumnSet",
    "ColumnType",
    "Date",
    "DateTime",
    "Enum",
    "FixedString",
    "FlattenedColumn",
    "Float",
    "IPv4",
    "IPv6",
    "Nested",
    "Nullable",
    "QualifiedColumnSet",
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
        self.columns = Column.to_columns(columns)

        self._lookup: MutableMapping[str, FlattenedColumn] = {}
        self._flattened: List[FlattenedColumn] = []
        for column in self.columns:
            self._flattened.extend(column.type.flatten(column.name))

        for col in self._flattened:
            if col.flattened in self._lookup:
                raise RuntimeError("Duplicate column: {}".format(col.flattened))

            self._lookup[col.flattened] = col
            # also store it by the escaped name
            self._lookup[col.escaped] = col

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

    def __contains__(self, key: str) -> bool:
        return key in self._lookup

    def __getitem__(self, key: str) -> FlattenedColumn:
        return self._lookup[key]

    def __iter__(self) -> Iterator[FlattenedColumn]:
        return iter(self._flattened)

    def get(
        self, key: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        try:
            return self[key]
        except KeyError:
            return default


class QualifiedColumnSet(ColumnSet):
    """
    Works like a Columnset but it represent a list of columns
    coming from different tables (like the ones we would use in
    a join).
    The main difference is that this class keeps track of the
    structure and to which table each column belongs to.
    """

    def __init__(self, column_sets: Mapping[str, ColumnSet]) -> None:
        # Iterate over the structured columns. get_columns() flattens nested
        # columns. We need them intact here.
        flat_columns = []
        for alias, column_set in column_sets.items():
            for column in column_set.columns:
                flat_columns.append((f"{alias}.{column.name}", column.type))
        super().__init__(flat_columns)

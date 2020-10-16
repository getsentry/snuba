from __future__ import annotations

from itertools import chain
from typing import (
    cast,
    Iterator,
    Mapping,
    MutableMapping,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from snuba.clickhouse.escaping import escape_identifier


class Column:
    def __init__(self, name: str, type: ColumnType) -> None:
        self.name = name
        self.type = type

    def __repr__(self) -> str:
        return "Column({}, {})".format(repr(self.name), repr(self.type))

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.name == cast(Column, other).name
            and self.type == cast(Column, other).type
        )

    def for_schema(self) -> str:
        return "{} {}".format(escape_identifier(self.name), self.type.for_schema())

    @staticmethod
    def to_columns(
        columns: Sequence[Union[Column, Tuple[str, ColumnType]]]
    ) -> Sequence[Column]:
        return [Column(*col) if not isinstance(col, Column) else col for col in columns]


class FlattenedColumn:
    def __init__(self, base_name: Optional[str], name: str, type: ColumnType) -> None:
        self.base_name = base_name
        self.name = name
        self.type = type

        self.flattened = (
            "{}.{}".format(self.base_name, self.name) if self.base_name else self.name
        )
        escaped = escape_identifier(self.flattened)
        assert escaped is not None
        self.escaped: str = escaped

    def __repr__(self) -> str:
        return "FlattenedColumn({}, {}, {})".format(
            repr(self.base_name), repr(self.name), repr(self.type)
        )

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.flattened == cast(FlattenedColumn, other).flattened
            and self.type == cast(FlattenedColumn, other).type
        )


class ColumnType:
    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"

    def __eq__(self, other: object) -> bool:
        return self.__class__ == other.__class__

    def for_schema(self) -> str:
        return self.__class__.__name__

    def flatten(self, name: str) -> Sequence[FlattenedColumn]:
        return [FlattenedColumn(None, name, self)]

    def get_raw(self) -> ColumnType:
        return self


class ReadOnly(ColumnType):
    def __init__(self, inner_type: ColumnType) -> None:
        self.inner_type = inner_type

    def __repr__(self) -> str:
        return "ReadOnly({})".format(repr(self.inner_type))

    def get_raw(self) -> ColumnType:
        return self.inner_type.get_raw()


class Nullable(ColumnType):
    def __init__(self, inner_type: ColumnType) -> None:
        self.inner_type = inner_type

    def __repr__(self) -> str:
        return "Nullable({})".format(repr(self.inner_type))

    def __eq__(self, other: object) -> bool:
        return (self.__class__ == other.__class__) and self.inner_type == cast(
            Nullable, other
        ).inner_type

    def for_schema(self) -> str:
        return "Nullable({})".format(self.inner_type.for_schema())

    def get_raw(self) -> ColumnType:
        return Nullable(self.inner_type.get_raw())


class Array(ColumnType):
    def __init__(self, inner_type: ColumnType) -> None:
        self.inner_type = inner_type

    def __repr__(self) -> str:
        return "Array({})".format(repr(self.inner_type))

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.inner_type == cast(Array, other).inner_type
        )

    def for_schema(self) -> str:
        return "Array({})".format(self.inner_type.for_schema())

    def get_raw(self) -> ColumnType:
        return Array(self.inner_type.get_raw())


class Nested(ColumnType):
    def __init__(
        self, nested_columns: Sequence[Union[Column, Tuple[str, ColumnType]]]
    ) -> None:
        self.nested_columns = Column.to_columns(nested_columns)

    def __repr__(self) -> str:
        return "Nested({})".format(repr(self.nested_columns))

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.nested_columns == cast(Nested, other).nested_columns
        )

    def for_schema(self) -> str:
        return "Nested({})".format(
            ", ".join(column.for_schema() for column in self.nested_columns)
        )

    def flatten(self, name: str) -> Sequence[FlattenedColumn]:
        return [
            FlattenedColumn(name, column.name, Array(column.type))
            for column in self.nested_columns
        ]


class AggregateFunction(ColumnType):
    def __init__(self, func: str, *arg_types: ColumnType) -> None:
        self.func = func
        self.arg_types = arg_types

    def __repr__(self) -> str:
        return "AggregateFunction({})".format(
            ", ".join(repr(x) for x in chain([self.func], self.arg_types)),
        )

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.func == cast(AggregateFunction, other).func
            and self.arg_types == cast(AggregateFunction, other).arg_types
        )

    def for_schema(self) -> str:
        return "AggregateFunction({})".format(
            ", ".join(chain([self.func], (x.for_schema() for x in self.arg_types))),
        )


class String(ColumnType):
    pass


class UUID(ColumnType):
    pass


class IPv4(ColumnType):
    pass


class IPv6(ColumnType):
    pass


class FixedString(ColumnType):
    def __init__(self, length: int) -> None:
        self.length = length

    def __repr__(self) -> str:
        return "FixedString({})".format(self.length)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.length == cast(FixedString, other).length
        )

    def for_schema(self) -> str:
        return "FixedString({})".format(self.length)


class UInt(ColumnType):
    def __init__(self, size: int) -> None:
        assert size in (8, 16, 32, 64)
        self.size = size

    def __repr__(self) -> str:
        return "UInt({})".format(self.size)

    def __eq__(self, other: object) -> bool:
        return self.__class__ == other.__class__ and self.size == cast(UInt, other).size

    def for_schema(self) -> str:
        return "UInt{}".format(self.size)


class Float(ColumnType):
    def __init__(self, size: int) -> None:
        assert size in (32, 64)
        self.size = size

    def __repr__(self) -> str:
        return "Float({})".format(self.size)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__ and self.size == cast(Float, other).size
        )

    def for_schema(self) -> str:
        return "Float{}".format(self.size)


class Date(ColumnType):
    pass


class DateTime(ColumnType):
    pass


class Enum(ColumnType):
    def __init__(self, values: Sequence[Tuple[str, int]]) -> None:
        self.values = values

    def __repr__(self) -> str:
        return "Enum({})".format(
            ", ".join("'{}' = {}".format(v[0], v[1]) for v in self.values)
        )

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.values == cast(Enum, other).values
        )

    def for_schema(self) -> str:
        return "Enum({})".format(
            ", ".join("'{}' = {}".format(v[0], v[1]) for v in self.values)
        )


class ColumnSet:
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
        self, columns: Sequence[Union[Column, Tuple[str, ColumnType]]]
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
        self, other: Union[ColumnSet, Sequence[Tuple[str, ColumnType]]]
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

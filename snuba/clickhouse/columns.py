from __future__ import annotations

from typing import Mapping, Optional, Sequence, Tuple, Union

from snuba.clickhouse.escaping import escape_identifier


class Column:
    def __init__(self, name: str, type: ColumnType) -> None:
        self.name = name
        self.type = type

    def __repr__(self) -> str:
        return "Column({}, {})".format(repr(self.name), repr(self.type))

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.name == other.name
            and self.type == other.type
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
        self.escaped = escape_identifier(self.flattened)

    def __repr__(self) -> str:
        return "FlattenedColumn({}, {}, {})".format(
            repr(self.base_name), repr(self.name), repr(self.type)
        )

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.flattened == other.flattened
            and self.type == other.type
        )


class ColumnType:
    def __repr__(self) -> str:
        return self.__class__.__name__ + "()"

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def for_schema(self) -> str:
        return self.__class__.__name__

    def flatten(self, name: str) -> Sequence[FlattenedColumn]:
        return [FlattenedColumn(None, name, self)]


class Nullable(ColumnType):
    def __init__(self, inner_type: ColumnType) -> None:
        self.inner_type = inner_type

    def __repr__(self) -> str:
        return "Nullable({})".format(repr(self.inner_type))

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.inner_type == other.inner_type

    def for_schema(self) -> str:
        return "Nullable({})".format(self.inner_type.for_schema())


class Materialized(ColumnType):
    def __init__(self, inner_type: ColumnType, expression: str) -> None:
        self.inner_type = inner_type
        self.expression = expression

    def __repr__(self) -> str:
        return "Materialized({}, {})".format(repr(self.inner_type), self.expression)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.expression == other.expression
            and self.inner_type == other.inner_type
        )

    def for_schema(self) -> str:
        return "{} MATERIALIZED {}".format(
            self.inner_type.for_schema(), self.expression,
        )


class WithDefault(ColumnType):
    def __init__(self, inner_type: ColumnType, default) -> None:
        self.inner_type = inner_type
        self.default = default  # XXX: this is problematic for typing

    def __repr__(self) -> str:
        return "WithDefault({}, {})".format(repr(self.inner_type), self.default)

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.default == other.default
            and self.inner_type == other.inner_type
        )

    def for_schema(self) -> str:
        return "{} DEFAULT {}".format(self.inner_type.for_schema(), self.default)


class Array(ColumnType):
    def __init__(self, inner_type: ColumnType) -> None:
        self.inner_type = inner_type

    def __repr__(self) -> str:
        return "Array({})".format(repr(self.inner_type))

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.inner_type == other.inner_type

    def for_schema(self) -> str:
        return "Array({})".format(self.inner_type.for_schema())


class Nested(ColumnType):
    def __init__(
        self, nested_columns: Sequence[Union[Column, Tuple[str, ColumnType]]]
    ) -> None:
        self.nested_columns = Column.to_columns(nested_columns)

    def __repr__(self) -> str:
        return "Nested({})".format(repr(self.nested_columns))

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.nested_columns == other.nested_columns
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


class LowCardinality(ColumnType):
    def __init__(self, inner_type: ColumnType) -> None:
        self.inner_type = inner_type

    def __repr__(self) -> str:
        return "LowCardinality({})".format(repr(self.inner_type))

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.inner_type == other.inner_type

    def for_schema(self) -> str:
        return "LowCardinality({})".format(self.inner_type.for_schema())


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

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.length == other.length

    def for_schema(self) -> str:
        return "FixedString({})".format(self.length)


class UInt(ColumnType):
    def __init__(self, size: int) -> None:
        assert size in (8, 16, 32, 64)
        self.size = size

    def __repr__(self) -> str:
        return "UInt({})".format(self.size)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.size == other.size

    def for_schema(self) -> str:
        return "UInt{}".format(self.size)


class Float(ColumnType):
    def __init__(self, size: int) -> None:
        assert size in (32, 64)
        self.size = size

    def __repr__(self) -> str:
        return "Float({})".format(self.size)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.size == other.size

    def for_schema(self) -> str:
        return "Float{}".format(self.size)


class DateTime(ColumnType):
    pass


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

        self._lookup = {}
        self._flattened = []
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

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._flattened == other._flattened

    def __len__(self) -> int:
        return len(self._flattened)

    def __add__(self, other) -> ColumnSet:
        if isinstance(other, ColumnSet):
            return ColumnSet(self.columns + other.columns)
        return ColumnSet(self.columns + other)

    def __contains__(self, key):
        return key in self._lookup

    def __getitem__(self, key):
        return self._lookup[key]

    def __iter__(self):
        return iter(self._flattened)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def for_schema(self) -> str:
        return ", ".join(column.for_schema() for column in self.columns)


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

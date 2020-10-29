from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import chain
from typing import (
    Generic,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from snuba.clickhouse.escaping import escape_identifier


class TypeModifier(ABC):
    """
    Changes the semantics of a ColumnType. Usual modifiers are
    Nullable, WithDefault, or similar.
    """

    @abstractmethod
    def for_schema(self, content: str) -> str:
        """
        Formats the modifier for DDL statements.
        The content parameter is the serialized type the modifier
        applies to.
        """
        raise NotImplementedError


class TypeModifiers(ABC):
    """
    Contains all the modifiers to apply to a type in a schema.
    An instance of this class is associated to a an instance of a
    ColumnType and provides the method to format the modifiers.

    The way instances of this class are initialized and the order
    in which modifiers are applied are up to the subclasses.
    """

    def for_schema(self, serialized_type: str) -> str:
        """
        Formats the modifiers around the pre-formatted ColumnType.
        """
        ret = serialized_type
        for c in self._get_modifiers():
            ret = c.for_schema(ret)
        return ret

    @abstractmethod
    def _get_modifiers(self) -> Sequence[TypeModifier]:
        """
        Establishes the order to follow when applying modifiers.
        """
        raise NotImplementedError

    def has_modifier(self, modifier: Type[TypeModifier]) -> bool:
        """
        Returns true if a modifier of the type provided is present in
        this container.
        """
        return any(t for t in self._get_modifiers() if isinstance(t, modifier))


TModifiers = TypeVar("TModifiers", bound=TypeModifiers)

# Unfortunately we cannot easily make these classes dataclasses (which
# would provide a convenient default implementation for all __repr__
# and __eq__ methods and allow for immutability) while keeping the
# schemas and migration concise.
# Making this a dataclass would mean `modifiers` would have to be the
# first argument of the constructor meaning that an empty list would
# have to be provided everytime we define a column in a migration or
# in a schema.
class ColumnType(Generic[TModifiers]):
    def __init__(self, modifiers: Optional[TModifiers] = None):
        self.__modifiers = modifiers

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._repr_content()})[{self.__modifiers}]"

    def _repr_content(self) -> str:
        """
        Extend if the type you are building has additional parameters
        to show in the representation.
        """
        return ""

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.__modifiers == cast(ColumnType[TModifiers], other).get_modifiers()
        )

    def for_schema(self) -> str:
        return (
            self.__modifiers.for_schema(self._for_schema_impl())
            if self.__modifiers is not None
            else self._for_schema_impl()
        )

    def _for_schema_impl(self) -> str:
        """
        Provides the clickhouse representation of this type
        without including the modifiers.
        """
        return self.__class__.__name__

    def flatten(self, name: str) -> Sequence[FlattenedColumn]:
        return [FlattenedColumn(None, name, self)]

    def get_modifiers(self) -> Optional[TModifiers]:
        return self.__modifiers

    def set_modifiers(self, modifiers: Optional[TModifiers]) -> ColumnType[TModifiers]:
        """
        Returns a new instance of this class with the provided
        modifiers.
        """
        return type(self)(modifiers=modifiers)

    def get_raw(self) -> ColumnType[TModifiers]:
        return type(self)()

    def has_modifier(self, modifier: Type[TypeModifier]) -> bool:
        if self.__modifiers is None:
            return False
        return self.__modifiers.has_modifier(modifier)


class Column(Generic[TModifiers]):
    def __init__(self, name: str, type: ColumnType[TModifiers]) -> None:
        self.name = name
        self.type = type

    def __repr__(self) -> str:
        return "Column({}, {})".format(repr(self.name), repr(self.type))

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.name == cast(Column[TModifiers], other).name
            and self.type == cast(Column[TModifiers], other).type
        )

    def for_schema(self) -> str:
        return "{} {}".format(escape_identifier(self.name), self.type.for_schema())

    @staticmethod
    def to_columns(
        columns: Sequence[Union[Column[TModifiers], Tuple[str, ColumnType[TModifiers]]]]
    ) -> Sequence[Column[TModifiers]]:
        return [Column(*col) if not isinstance(col, Column) else col for col in columns]


class FlattenedColumn:
    def __init__(
        self, base_name: Optional[str], name: str, type: ColumnType[TModifiers]
    ) -> None:
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


@dataclass(frozen=True)
class SchemaModifiers(TypeModifiers):
    """
    Modifiers for the schemas we use during query processing.
    """

    nullable: bool = False
    readonly: bool = False

    def _get_modifiers(self) -> Sequence[TypeModifier]:
        ret: List[TypeModifier] = []
        if self.nullable:
            ret.append(Nullable())
        if self.readonly:
            ret.append(ReadOnly())
        return ret


@dataclass(frozen=True)
class ReadOnly(TypeModifier):
    def for_schema(self, content: str) -> str:
        return content


@dataclass(frozen=True)
class Nullable(TypeModifier):
    def for_schema(self, content: str) -> str:
        return "Nullable({})".format(content)


class Array(ColumnType[TModifiers]):
    def __init__(
        self, inner_type: ColumnType[TModifiers], modifiers: Optional[TModifiers] = None
    ) -> None:
        super().__init__(modifiers)
        self.inner_type = inner_type

    def _repr_content(self) -> str:
        return repr(self.inner_type)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.inner_type == cast(Array[TModifiers], other).inner_type
            and self.get_modifiers() == cast(Array[TModifiers], other).get_modifiers()
        )

    def _for_schema_impl(self) -> str:
        return "Array({})".format(self.inner_type.for_schema())

    def set_modifiers(self, modifiers: Optional[TModifiers]) -> Array[TModifiers]:
        return Array(inner_type=self.inner_type, modifiers=modifiers)

    def get_raw(self) -> Array[TModifiers]:
        return Array(inner_type=self.inner_type.get_raw())


class Nested(ColumnType[TModifiers]):
    def __init__(
        self,
        nested_columns: Sequence[
            Union[Column[TModifiers], Tuple[str, ColumnType[TModifiers]]]
        ],
        modifiers: Optional[TModifiers] = None,
    ) -> None:
        super().__init__(modifiers)
        self.nested_columns = Column.to_columns(nested_columns)

    def _repr_content(self) -> str:
        return repr(self.nested_columns)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.get_modifiers() == cast(Nested[TModifiers], other).get_modifiers()
            and self.nested_columns == cast(Nested[TModifiers], other).nested_columns
        )

    def _for_schema_impl(self) -> str:
        return "Nested({})".format(
            ", ".join(column.for_schema() for column in self.nested_columns)
        )

    def flatten(self, name: str) -> Sequence[FlattenedColumn]:
        return [
            FlattenedColumn(name, column.name, Array(column.type))
            for column in self.nested_columns
        ]

    def set_modifiers(self, modifiers: Optional[TModifiers]) -> Nested[TModifiers]:
        return Nested(nested_columns=self.nested_columns, modifiers=modifiers)

    def get_raw(self) -> Nested[TModifiers]:
        raw_columns = [Column(c.name, c.type.get_raw()) for c in self.nested_columns]
        return Nested(nested_columns=raw_columns)


class AggregateFunction(ColumnType[TModifiers]):
    def __init__(
        self,
        func: str,
        arg_types: Sequence[ColumnType[TModifiers]],
        modifiers: Optional[TModifiers] = None,
    ) -> None:
        super().__init__(modifiers)
        self.func = func
        self.arg_types = arg_types

    def _repr_content(self) -> str:
        return ", ".join(repr(x) for x in chain([self.func], self.arg_types))

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.get_modifiers()
            == cast(AggregateFunction[TModifiers], other).get_modifiers()
            and self.func == cast(AggregateFunction[TModifiers], other).func
            and self.arg_types == cast(AggregateFunction[TModifiers], other).arg_types
        )

    def _for_schema_impl(self) -> str:
        return "AggregateFunction({})".format(
            ", ".join(chain([self.func], (x.for_schema() for x in self.arg_types))),
        )

    def set_modifiers(
        self, modifiers: Optional[TModifiers]
    ) -> AggregateFunction[TModifiers]:
        return AggregateFunction(self.func, self.arg_types, modifiers)

    def get_raw(self) -> AggregateFunction[TModifiers]:
        return AggregateFunction(self.func, [t.get_raw() for t in self.arg_types])


class String(ColumnType[TModifiers]):
    pass


class UUID(ColumnType[TModifiers]):
    pass


class IPv4(ColumnType[TModifiers]):
    pass


class IPv6(ColumnType[TModifiers]):
    pass


class FixedString(ColumnType[TModifiers]):
    def __init__(self, length: int, modifiers: Optional[TModifiers] = None) -> None:
        super().__init__(modifiers)
        self.length = length

    def _repr_content(self) -> str:
        return str(self.length)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.get_modifiers()
            == cast(FixedString[TModifiers], other).get_modifiers()
            and self.length == cast(FixedString[TModifiers], other).length
        )

    def _for_schema_impl(self) -> str:
        return "FixedString({})".format(self.length)

    def set_modifiers(self, modifiers: Optional[TModifiers]) -> FixedString[TModifiers]:
        return FixedString(length=self.length, modifiers=modifiers)

    def get_raw(self) -> FixedString[TModifiers]:
        return FixedString(self.length)


class UInt(ColumnType[TModifiers]):
    def __init__(self, size: int, modifiers: Optional[TModifiers] = None) -> None:
        super().__init__(modifiers)
        assert size in (8, 16, 32, 64)
        self.size = size

    def _repr_content(self) -> str:
        return str(self.size)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.get_modifiers() == cast(UInt[TModifiers], other).get_modifiers()
            and self.size == cast(UInt[TModifiers], other).size
        )

    def _for_schema_impl(self) -> str:
        return "UInt{}".format(self.size)

    def set_modifiers(self, modifiers: Optional[TModifiers]) -> UInt[TModifiers]:
        return UInt(size=self.size, modifiers=modifiers)

    def get_raw(self) -> UInt[TModifiers]:
        return UInt(self.size)


class Float(ColumnType[TModifiers]):
    def __init__(self, size: int, modifiers: Optional[TModifiers] = None,) -> None:
        super().__init__(modifiers)
        assert size in (32, 64)
        self.size = size

    def _repr_content(self) -> str:
        return str(self.size)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.get_modifiers() == cast(Float[TModifiers], other).get_modifiers()
            and self.size == cast(Float[TModifiers], other).size
        )

    def _for_schema_impl(self) -> str:
        return "Float{}".format(self.size)

    def set_modifiers(self, modifiers: Optional[TModifiers]) -> Float[TModifiers]:
        return Float(size=self.size, modifiers=modifiers)

    def get_raw(self) -> Float[TModifiers]:
        return Float(self.size)


class Date(ColumnType[TModifiers]):
    pass


class DateTime(ColumnType[TModifiers]):
    pass


class Enum(ColumnType[TModifiers]):
    def __init__(
        self, values: Sequence[Tuple[str, int]], modifiers: Optional[TModifiers] = None,
    ) -> None:
        super().__init__(modifiers)
        self.values = values

    def _repr_content(self) -> str:
        return ", ".join("'{}' = {}".format(v[0], v[1]) for v in self.values)

    def __eq__(self, other: object) -> bool:
        return (
            self.__class__ == other.__class__
            and self.get_modifiers() == cast(Enum[TModifiers], other).get_modifiers()
            and self.values == cast(Enum[TModifiers], other).values
        )

    def _for_schema_impl(self) -> str:
        return "Enum({})".format(
            ", ".join("'{}' = {}".format(v[0], v[1]) for v in self.values)
        )

    def set_modifiers(self, modifiers: Optional[TModifiers]) -> Enum[TModifiers]:
        return Enum(values=self.values, modifiers=modifiers)

    def get_raw(self) -> Enum[TModifiers]:
        return Enum(self.values)


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

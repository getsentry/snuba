from abc import ABC, abstractmethod
from typing import (
    Iterator,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    MutableMapping,
    List,
)

from snuba.clickhouse.columns import Column, FlattenedColumn, SchemaModifiers

ColumnType = TypeVar("ColumnType")


class ColumnSet(ABC):
    """
    Base column set extended by both ClickHouse column set and entity column set
    """

    def __init__(self, columns: Sequence[Column[SchemaModifiers]]) -> None:
        self.columns = columns

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

    def __getitem__(self, key: str) -> FlattenedColumn:
        return self._lookup[key]

    def get(
        self, key: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        return key in self._lookup

    def __iter__(self) -> Iterator[FlattenedColumn]:
        return iter(self._flattened)


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

        super().__init__(Column.to_columns(flat_columns))

    def get(
        self, key: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        # get from self.__flat_columns
        pass

    def __iter__(self) -> Iterator[FlattenedColumn]:
        for column in self._flattened:
            yield column

    def __contains__(self, key: str) -> bool:
        # TODO: Wildcard column
        return key in self._lookup


class DataSource(ABC):
    """
    Represents the source of the records a query (or a portion of it)
    acts upon.
    In the most common case this is the FROM clause but it can be used
    in other sections of the query for subqueries.
    """

    @abstractmethod
    def get_columns(self) -> ColumnSet:
        raise NotImplementedError

from abc import ABC, abstractmethod
from typing import Iterator, Mapping, Optional, Sequence, TypeVar

from snuba.clickhouse.columns import Column, FlattenedColumn, SchemaModifiers

ColumnType = TypeVar("ColumnType")


class ColumnSet(ABC):
    @property
    def columns(self) -> Sequence[Column[SchemaModifiers]]:
        raise NotImplementedError

    @abstractmethod
    def get(
        self, key: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[FlattenedColumn]:
        raise NotImplementedError

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        raise NotImplementedError


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
        self.__flat_columns = []
        for alias, column_set in column_sets.items():
            for column in column_set.columns:
                self.__flat_columns.append((f"{alias}.{column.name}", column.type))
        # super().__init__(flat_columns)

    def get(
        self, key: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        # get from self.__flat_columns
        pass


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

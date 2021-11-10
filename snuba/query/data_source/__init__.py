from abc import ABC, abstractmethod
from typing import Iterator, Mapping, Optional

from snuba.clickhouse.columns import Column, FlattenedColumn
from snuba.datasets.entities.entity_data_model import WildcardColumn
from snuba.utils.schemas import ColumnSet


class QualifiedColumnSet(ColumnSet):
    """
    Works like a Columnset but it represent a list of columns
    coming from different tables (like the ones we would use in
    a join).
    The main difference is that this class keeps track of the
    structure and to which table each column belongs to.
    """

    def __init__(self, column_sets: Mapping[str, ColumnSet]) -> None:
        flat_columns = []
        wildcard_columns = []

        for alias, column_set in column_sets.items():
            for column in column_set.columns:
                if isinstance(column.type, WildcardColumn):
                    wildcard_columns.append((f"{alias}.{column.name}", column.type))
                else:
                    flat_columns.append((f"{alias}.{column.name}", column.type))

        super().__init__(Column.to_columns(flat_columns))

    def __getitem__(self, key: str) -> FlattenedColumn:
        return self._lookup[key]

    def get(
        self, key: str, default: Optional[FlattenedColumn] = None
    ) -> Optional[FlattenedColumn]:
        try:
            return self[key]
        except KeyError:
            return default

    def __iter__(self) -> Iterator[FlattenedColumn]:
        for column in self._flattened:
            yield column


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

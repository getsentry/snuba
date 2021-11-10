from abc import ABC, abstractmethod
from typing import Mapping, MutableSequence

from snuba.clickhouse.columns import Column
from snuba.utils.schemas import ColumnSet, SchemaModifiers, WildcardColumn


class QualifiedColumnSet(ColumnSet):
    """
    Works like a Columnset but it represent a list of columns
    coming from different tables (like the ones we would use in
    a join).
    The main difference is that this class keeps track of the
    structure and to which table each column belongs to.
    """

    def __init__(self, column_sets: Mapping[str, ColumnSet]) -> None:
        columns: MutableSequence[Column[SchemaModifiers]] = []

        for alias, column_set in column_sets.items():
            for column in column_set.columns:
                if isinstance(column, WildcardColumn):
                    columns.append(
                        WildcardColumn(f"{alias}.{column.name}", column.type)
                    )
                else:
                    columns.append(Column(f"{alias}.{column.name}", column.type))

        super().__init__(columns)


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

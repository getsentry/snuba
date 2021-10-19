from abc import ABC, abstractmethod
from snuba.clickhouse.columns import ColumnSet


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

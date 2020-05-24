from abc import ABC, abstractmethod
from typing import NamedTuple, Optional, Sequence

from snuba.clickhouse.columns import ColumnSet
from snuba.query.expressions import Column


ColumnParts = Sequence[str]


class ResolvedCol(NamedTuple):
    table_name: Optional[str]
    column_name: str
    path: Sequence[str]


class ColumnResolver(ABC):
    """
    A ColumnResolver is a component that knows the logical schema of a data set and is capable
    of resolving entities and nested column from the representation that we receive in the query.

    This assumes the parser does not have enough information from the query language alone to
    decompose a column expression found in a query into entity (table), base column name and
    path for nested columns.

    TODO: Revisit this interface when we introduce entities. We may have to increase its
    responsibilities depending on how we decide to infer, given a column, which entity defines it.
    If we require the query to fully qualify all columns (errors.message for example), this can
    stay as it is. If instead we will infer the requested entity from the context of the query
    like we do today, this will need to know a lot more about the query, be coupled to that
    structure and being potentially able to make wider changes to the query itself.
    Moving such responsibility here would be easy enough that we can keep this simpler till
    entities are introduced.
    """

    @abstractmethod
    def resolve_column(self, query_column: str) -> Optional[ResolvedCol]:
        """
        Transforms the column parts found in the query (like ["events", "tags", "value"]) into
        a valid Column object for the logical AST. If the column cannot be resolved, this
        returns None.
        """
        raise NotImplementedError


class SingleTableResolver(ColumnResolver):
    def __init__(
        self,
        columns: ColumnSet,
        virtual_column_names: Sequence[str],
        table_name: Optional[str] = None,
    ) -> None:
        self.__columns = columns
        self.__virtual_column_names = virtual_column_names
        self.__table_name = table_name

    def resolve_column(self, query_column: str) -> Optional[Column]:
        schema_column = self.__columns.columns.get(query_column)
        if schema_column:
            return ResolvedCol(table_name=self.__table_name,)

        elif query_column[0] in self.__virtual_column_names:
            pass
        else:
            return None

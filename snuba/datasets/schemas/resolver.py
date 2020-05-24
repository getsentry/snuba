from abc import ABC, abstractmethod
from typing import Optional, Sequence

from snuba.query.expressions import Column

ColumnParts = Sequence[str]


class ColumnResolver(ABC):
    """
    A ColumnResolver is a component that knows the logical schema of a data set and is capable
    of resolving entities and nested column from the representation that we receive in the query.

    This assumes the parser does not have enough information from the query language alone to
    decompose a column expression found in a query into entity (table), base column name and
    path for nested columns.

    In order to keep the coupling with the query language minimum, this expects the parser to
    provide a structured representation of a column as a sequence of strings instead of the bare
    representation (string) found in the query.
    Still this cannot be totally decoupled from the query language.
    """

    @abstractmethod
    def resolve_column(self, query_column: ColumnParts) -> Optional[Column]:
        """
        Transforms the column parts found in the query (like ["events", "tags", "value"]) into
        a valid Column object for the logical AST. If the column cannot be resolved, this
        returns None.
        """
        raise NotImplementedError

from abc import ABC, abstractmethod
from typing import Optional


class ClickhouseSqlQuery(ABC):
    """
    An abstraction that provides the formatted SQL query to execute on Clickhouse.
    This class embeds (until the AST is not done so we can remove the Dict based formatter)
    the formatting logic that transforms a Clickhouse Query into a SQL string.

    This abstraction should not be needed. What we would need is a formatter that transforms
    the Clickhouse Query into a string that can be used directly by the reader.
    As long as we have DictClickhouseSqlQuery around this is not possible because
    in the old representation, query processing and formatting are mixed with each other
    and, worse, they are stateful (we cannot format the query twice). Once that class
    is gone we can turn this into a stateless formatter.
    """

    @abstractmethod
    def _format_query_impl(self) -> str:
        """
        Produces the SQL representation of this query without the ``FORMAT``
        clause. Not intended to be used by external callers, but must be
        implemented by subclasses to enable ``format_sql`` to function.
        """
        raise NotImplementedError

    def format_sql(self, format: Optional[str] = None) -> str:
        """Produces the SQL representation of this query."""
        query = self._format_query_impl()
        if format is not None:
            query = f"{query} FORMAT {format}"
        return query

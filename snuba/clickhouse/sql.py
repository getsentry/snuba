from abc import ABC, abstractmethod
from typing import Mapping, Optional


class SqlQuery(ABC):
    """
    An abstraction that provides a formatted SQL query to execute on Clickhouse.
    This class embeds the formatting logic that transforms a Clickhouse Query into a SQL string.
    The way this formatting is performed depends on the implementations.

    This abstraction should not be needed. What we would need is a stateless formatter that
    transforms the Clickhouse Query into a string and that can be used directly by the reader.
    As long as we have DictSqlQuery around, this is not achievable because,
    in the old representation, query processing and formatting are mixed with each other
    and, worse, they are stateful (we cannot format the query twice).
    As a consequence, we ned to instantiate this class before we get to the reader and pass it
    around so it guarantees formatting happens only once. Once that class is gone we can turn
    this into a stateless formatter.
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

    @abstractmethod
    def sql_data(self) -> Mapping[str, str]:
        """Returns a lust of parsed SQL elements from this query"""
        raise NotImplementedError

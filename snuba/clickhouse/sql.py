from abc import ABC, abstractmethod
from typing import Mapping, Optional


class SqlQuery(ABC):
    """
    An abstraction that provides a formatted SQL query to execute on
    Clickhouse.
    This class embeds the formatting logic that transforms a Clickhouse
    Query into a SQL string.
    The way this formatting is performed depends on the implementations.

    TODO:
    This abstraction is obsolete and should be refactored.
    Now it takes a Clickhouse query and it is capable to produce the
    SQL query in different formats. This is still useful, but it should
    replace the clickhouse query where it is used.
    """

    @abstractmethod
    def format_sql(self, format: Optional[str] = None) -> str:
        """Produces the SQL representation of this query."""
        raise NotImplementedError

    @abstractmethod
    def sql_data(self) -> Mapping[str, str]:
        """Returns a lust of parsed SQL elements from this query"""
        raise NotImplementedError

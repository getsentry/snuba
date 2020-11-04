from typing import Mapping, Optional

from snuba import settings as snuba_settings
from snuba.clickhouse.query_formatter import format_query
from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.request.request_settings import RequestSettings


class AstSqlQuery(SqlQuery):
    """
    SqlQuery implementation that builds the SQL query out of the
    AST representation present in the Clickhouse Query object.
    """

    def __init__(self, query: Query, settings: RequestSettings,) -> None:
        self.__data_source = query.get_from_clause()
        if not self.__data_source.supports_sample():
            sample_rate = None
        else:
            if query.get_sample():
                sample_rate = query.get_sample()
            elif settings.get_turbo():
                sample_rate = snuba_settings.TURBO_SAMPLE_RATE
            else:
                sample_rate = None
        query.set_sample(sample_rate)

        self.__formatted_query = format_query(query)

    def format_sql(self, format: Optional[str] = None) -> str:
        return self.__formatted_query.get_sql(format)

    def sql_data(self) -> Mapping[str, str]:
        return self.__formatted_query.get_mapping()

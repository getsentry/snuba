from typing import Optional

from snuba.clickhouse.query import ClickhouseQuery
from snuba.query.expressions import Expression
from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings


class AstClickhouseQuery(ClickhouseQuery):
    """
    Clickhouse query that takes the content from the Snuba Query
    AST and can be processed (through query processors) for Clickhouse
    specific customizations.

    Here the process of formatting the query, is independent from
    the query body dictionary and it is performed starting from the
    AST.
    """

    def __init__(self, query: Query, settings: RequestSettings,) -> None:
        # Snuba query structure
        # Referencing them here directly since it makes it easier
        # to process this query independently from the Snuba Query
        # and there is no risk in doing so since they are immutable.
        self.__selected_columns = query.get_selected_columns_from_ast()
        self.__condition = query.get_conditions_from_ast()
        self.__groupby = query.get_groupby_from_ast()
        self.__having = query.get_having_from_ast()
        self.__orderby = query.get_orderby_from_ast()
        self.__data_source = query.get_data_source()
        self.__arrayjoin = query.get_arrayjoin_from_ast()
        self.__granularity = query.get_granularity()
        self.__limit = query.get_limit()
        self.__limitby = query.get_limitby()
        self.__offset = query.get_offset()

        # Clickhouse specific fields
        self.__prewhere: Optional[Expression] = None
        self.__final = settings.get_turbo()

        # Attributes that should be clickhouse specific that
        # have to be removed from the Snuba Query
        self.__sample = query.get_sample()
        self.__hastotals = query.has_totals()

    def format_sql(self) -> str:
        """
        TODO: Do something interesting here.
        """
        raise NotImplementedError

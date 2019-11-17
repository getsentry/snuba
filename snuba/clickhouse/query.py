from abc import ABC, abstractmethod
from typing import Optional

from snuba import settings as snuba_settings
from snuba import util
from snuba.query.columns import (
    column_expr,
    conditions_expr,
)
from snuba.datasets.dataset import Dataset
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.query.expressions import Expression
from snuba.request.request_settings import RequestSettings


class ClickhouseQuery(ABC):
    """
    Generates and represents a Clickhouse query from a Request
    and a snuba Query
    """

    @abstractmethod
    def format_sql(self) -> str:
        raise NotImplementedError


class DictClickhouseQuery(ClickhouseQuery):
    """
    Legacy Clickhouse query that transforms the Snuba Query based
    on the original query body dictionary into a string dunring construction
    without additional processing.

    To be used until the AST is not complete.
    """

    def __init__(self,
        dataset: Dataset,
        query: Query,
        settings: RequestSettings,
    ) -> None:
        parsing_context = ParsingContext()

        aggregate_exprs = [column_expr(dataset, col, query, parsing_context, alias, agg) for (agg, col, alias) in query.get_aggregations()]
        groupby = util.to_list(query.get_groupby())
        group_exprs = [column_expr(dataset, gb, query, parsing_context) for gb in groupby]
        column_names = query.get_selected_columns() or []
        selected_cols = [column_expr(dataset, util.tuplify(colname), query, parsing_context) for colname in column_names]
        select_clause = u'SELECT {}'.format(', '.join(group_exprs + aggregate_exprs + selected_cols))

        from_clause = u'FROM {}'.format(query.get_data_source().format_from())

        if query.get_final():
            from_clause = u'{} FINAL'.format(from_clause)

        if not query.get_data_source().supports_sample():
            sample_rate = None
        else:
            if query.get_sample():
                sample_rate = query.get_sample()
            elif settings.get_turbo():
                sample_rate = snuba_settings.TURBO_SAMPLE_RATE
            else:
                sample_rate = None

        if sample_rate:
            from_clause = u'{} SAMPLE {}'.format(from_clause, sample_rate)

        join_clause = ''
        if query.get_arrayjoin():
            join_clause = u'ARRAY JOIN {}'.format(query.get_arrayjoin())

        where_clause = ''
        if query.get_conditions():
            where_clause = u'WHERE {}'.format(conditions_expr(dataset, query.get_conditions(), query, parsing_context))

        prewhere_clause = ''
        if query.get_prewhere():
            prewhere_clause = u'PREWHERE {}'.format(conditions_expr(dataset, query.get_prewhere(), query, parsing_context))

        group_clause = ''
        if groupby:
            group_clause = 'GROUP BY ({})'.format(', '.join(column_expr(dataset, gb, query, parsing_context) for gb in groupby))
            if query.has_totals():
                group_clause = '{} WITH TOTALS'.format(group_clause)

        having_clause = ''
        having_conditions = query.get_having()
        if having_conditions:
            assert groupby, 'found HAVING clause with no GROUP BY'
            having_clause = u'HAVING {}'.format(conditions_expr(dataset, having_conditions, query, parsing_context))

        order_clause = ''
        if query.get_orderby():
            orderby = [column_expr(dataset, util.tuplify(ob), query, parsing_context) for ob in util.to_list(query.get_orderby())]
            orderby = [u'{} {}'.format(ob.lstrip('-'), 'DESC' if ob.startswith('-') else 'ASC') for ob in orderby]
            order_clause = u'ORDER BY {}'.format(', '.join(orderby))

        limitby_clause = ''
        if query.get_limitby() is not None:
            limitby_clause = 'LIMIT {} BY {}'.format(*query.get_limitby())

        limit_clause = ''
        if query.get_limit() is not None:
            limit_clause = 'LIMIT {}, {}'.format(query.get_offset(), query.get_limit())

        self.__formatted_query = ' '.join([c for c in [
            select_clause,
            from_clause,
            join_clause,
            prewhere_clause,
            where_clause,
            group_clause,
            having_clause,
            order_clause,
            limitby_clause,
            limit_clause
        ] if c])

    def format_sql(self) -> str:
        """Produces a SQL string from the parameters."""
        return self.__formatted_query


class AstClickhouseQuery(ClickhouseQuery):
    """
    Clickhouse query that takes the content from the Snuba Query
    AST and can be processed (through query processors) for Clickhouse
    specific customizations.

    Here the process of formatting the query, is independent from
    the query body dictionary and it is performed starting from the
    AST.
    """

    def __init__(self,
        query: Query,
        settings: RequestSettings,
    ) -> None:
        # Snuba query structure
        # Referencing them here directly since it makes it easier
        # to process this query independently from the Snuba Query
        # and there is no risk in doing so since they are immutable.
        self.__selected_columns = query.get_selected_columns_exp()
        self.__condition = query.get_conditions_exp()
        self.__groupby = query.get_groupby_exp()
        self.__having = query.get_having_exp()
        self.__orderby = query.get_orderby_exp()
        self.__data_source = query.get_data_source()
        self.__arrayjoin = query.get_arrayjoin_exp()
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

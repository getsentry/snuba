from typing import Sequence

from snuba import settings as snuba_settings
from snuba import util
from snuba.datasets import Dataset
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings


class ClickhouseQuery:
    """
    Generates and represents a Clickhouse query from a Request
    and a snuba Query
    """

    def __init__(self,
        dataset: Dataset,
        query: Query,
        settings: RequestSettings,
        prewhere_conditions: Sequence[str],
    ) -> None:
        source = dataset \
            .get_dataset_schemas() \
            .get_read_schema() \
            .get_data_source()

        parsing_context = ParsingContext()

        aggregate_exprs = [util.column_expr(dataset, col, query, parsing_context, alias, agg) for (agg, col, alias) in query.get_aggregations()]
        groupby = util.to_list(query.get_groupby())
        group_exprs = [util.column_expr(dataset, gb, query, parsing_context) for gb in groupby]
        column_names = query.get_selected_columns() or []
        selected_cols = [util.column_expr(dataset, util.tuplify(colname), query, parsing_context) for colname in column_names]
        select_clause = u'SELECT {}'.format(', '.join(group_exprs + aggregate_exprs + selected_cols))

        from_clause = u'FROM {}'.format(source)

        if query.get_final():
            from_clause = u'{} FINAL'.format(from_clause)

        if query.get_sample():
            sample_rate = query.get_sample()
        elif settings.turbo:
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
            where_clause = u'WHERE {}'.format(util.conditions_expr(dataset, query.get_conditions(), query, parsing_context))

        prewhere_clause = ''
        if prewhere_conditions:
            prewhere_clause = u'PREWHERE {}'.format(util.conditions_expr(dataset, prewhere_conditions, query, parsing_context))

        group_clause = ''
        if groupby:
            group_clause = 'GROUP BY ({})'.format(', '.join(util.column_expr(dataset, gb, query, parsing_context) for gb in groupby))
            if query.has_totals():
                group_clause = '{} WITH TOTALS'.format(group_clause)

        having_clause = ''
        having_conditions = query.get_having()
        if having_conditions:
            assert groupby, 'found HAVING clause with no GROUP BY'
            having_clause = u'HAVING {}'.format(util.conditions_expr(dataset, having_conditions, query, parsing_context))

        order_clause = ''
        if query.get_orderby():
            orderby = [util.column_expr(dataset, util.tuplify(ob), query, parsing_context) for ob in util.to_list(query.get_orderby())]
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

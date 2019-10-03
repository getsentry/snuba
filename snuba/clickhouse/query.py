from typing import Sequence

from snuba import util, settings
from snuba.datasets import Dataset
from snuba.query.parsing import ParsingContext
from snuba.request import Request


class ClickhouseQuery:
    def __init__(self,
        dataset: Dataset,
        # TODO: pass query only once util.column_expr will not depend on the full request
        # body anymore.
        request: Request,
        prewhere_conditions: Sequence[str],
    ) -> None:
        self.__dataset = dataset
        self.__request = request
        self.__prewhere_conditions = prewhere_conditions

    def format(self) -> str:
        """Generate a SQL string from the parameters."""
        body = self.__request.body
        query = self.__request.query
        source = self.__dataset \
            .get_dataset_schemas() \
            .get_read_schema() \
            .get_data_source()

        parsing_context = ParsingContext()

        aggregate_exprs = [util.column_expr(self.__dataset, col, body, parsing_context, alias, agg) for (agg, col, alias) in query.get_aggregations()]
        groupby = util.to_list(query.get_groupby())
        group_exprs = [util.column_expr(self.__dataset, gb, body, parsing_context) for gb in groupby]
        column_names = query.get_selected_columns() or []
        selected_cols = [util.column_expr(self.__dataset, util.tuplify(colname), body, parsing_context) for colname in column_names]
        select_clause = u'SELECT {}'.format(', '.join(group_exprs + aggregate_exprs + selected_cols))

        from_clause = u'FROM {}'.format(source)

        if self.__request.query.get_final():
            from_clause = u'{} FINAL'.format(from_clause)

        if query.get_sample():
            sample_rate = query.get_sample()
        elif self.__request.settings.turbo:
            sample_rate = settings.TURBO_SAMPLE_RATE
        else:
            sample_rate = None

        if sample_rate:
            from_clause = u'{} SAMPLE {}'.format(from_clause, sample_rate)

        join_clause = ''
        if query.get_arrayjoin():
            join_clause = u'ARRAY JOIN {}'.format(query.get_arrayjoin())

        where_clause = ''
        if query.get_conditions():
            where_clause = u'WHERE {}'.format(util.conditions_expr(self.__dataset, query.get_conditions(), body, parsing_context))

        prewhere_clause = ''
        if self.__prewhere_conditions:
            prewhere_clause = u'PREWHERE {}'.format(util.conditions_expr(self.__dataset, self.__prewhere_conditions, body, parsing_context))

        group_clause = ''
        if groupby:
            group_clause = 'GROUP BY ({})'.format(', '.join(util.column_expr(self.__dataset, gb, body, parsing_context) for gb in groupby))
            if query.has_totals():
                group_clause = '{} WITH TOTALS'.format(group_clause)

        having_clause = ''
        having_conditions = query.get_having()
        if having_conditions:
            assert groupby, 'found HAVING clause with no GROUP BY'
            having_clause = u'HAVING {}'.format(util.conditions_expr(self.__dataset, having_conditions, body, parsing_context))

        order_clause = ''
        if query.get_orderby():
            orderby = [util.column_expr(self.__dataset, util.tuplify(ob), body, parsing_context) for ob in util.to_list(query.get_orderby())]
            orderby = [u'{} {}'.format(ob.lstrip('-'), 'DESC' if ob.startswith('-') else 'ASC') for ob in orderby]
            order_clause = u'ORDER BY {}'.format(', '.join(orderby))

        limitby_clause = ''
        if query.get_limitby() is not None:
            limitby_clause = 'LIMIT {} BY {}'.format(*query.get_limitby())

        limit_clause = ''
        if query.get_limit() is not None:
            limit_clause = 'LIMIT {}, {}'.format(query.get_offset(), query.get_limit())

        return ' '.join([c for c in [
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

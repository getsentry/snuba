from typing import Sequence

from snuba import util, settings
from snuba.datasets import Dataset
from snuba.request import Request
from snuba.query.query import QueryHints


class ClickhouseQuery:
    def __init__(self,
        dataset: Dataset,
        # TODO: pass query only once util.column_expr will not depend on the full request
        # body anymore.
        request: Request,
        prewhere_conditions: Sequence[str],
        query_hints: QueryHints,
    ) -> None:
        self.__dataset = dataset
        self.__request = request
        self.__prewhere_conditions = prewhere_conditions
        self.__query_hints = query_hints

    def format(self) -> str:
        """Generate a SQL string from the parameters."""
        body = self.__request.body
        query = self.__request.query
        source = self.__dataset \
            .get_dataset_schemas() \
            .get_read_schema() \
            .get_data_source()

        aggregate_exprs = [util.column_expr(self.__dataset, col, body, alias, agg) for (agg, col, alias) in query.get_aggregations()]
        groupby = util.to_list(query.get_groupby())
        group_exprs = [util.column_expr(self.__dataset, gb, body) for gb in groupby]
        column_names = query.get_selected_columns() or []
        selected_cols = [util.column_expr(self.__dataset, util.tuplify(colname), body) for colname in column_names]
        select_clause = u'SELECT {}'.format(', '.join(group_exprs + aggregate_exprs + selected_cols))

        from_clause = u'FROM {}'.format(source)

        if self.__query_hints.final:
            from_clause = u'{} FINAL'.format(from_clause)

        if query.get_sample():
            from_clause = u'{} SAMPLE {}'.format(from_clause, query.get_sample())

        join_clause = ''
        if 'arrayjoin' in body:
            join_clause = u'ARRAY JOIN {}'.format(body['arrayjoin'])

        where_clause = ''
        if query.get_conditions():
            where_clause = u'WHERE {}'.format(util.conditions_expr(self.__dataset, query.get_conditions(), body))

        prewhere_clause = ''
        if self.__prewhere_conditions:
            prewhere_clause = u'PREWHERE {}'.format(util.conditions_expr(self.__dataset, self.__prewhere_conditions, body))

        group_clause = ''
        if groupby:
            group_clause = 'GROUP BY ({})'.format(', '.join(util.column_expr(self.__dataset, gb, body) for gb in groupby))
            if body.get('totals', False):
                group_clause = '{} WITH TOTALS'.format(group_clause)

        having_clause = ''
        having_conditions = body.get('having', [])
        if having_conditions:
            assert groupby, 'found HAVING clause with no GROUP BY'
            having_clause = u'HAVING {}'.format(util.conditions_expr(self.__dataset, having_conditions, body))

        order_clause = ''
        if query.get_orderby():
            orderby = [util.column_expr(self.__dataset, util.tuplify(ob), body) for ob in util.to_list(query.get_orderby())]
            orderby = [u'{} {}'.format(ob.lstrip('-'), 'DESC' if ob.startswith('-') else 'ASC') for ob in orderby]
            order_clause = u'ORDER BY {}'.format(', '.join(orderby))

        limitby_clause = ''
        if 'limitby' in body:
            limitby_clause = 'LIMIT {} BY {}'.format(*body['limitby'])

        limit_clause = ''
        if 'limit' in body:
            limit_clause = 'LIMIT {}, {}'.format(query.get_offset(), body['limit'])

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

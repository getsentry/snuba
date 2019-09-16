from typing import Sequence

from snuba import util
from snuba.datasets import Dataset
from snuba.request import Request


class ClickhouseQuery:
    def __init__(self,
        dataset: Dataset,
        # TODO: replace this with snuba.query. Query as soon as the PR is committed
        request: Request,
        prewhere_conditions: Sequence[str],
        final: bool,
    ) -> None:
        self.__dataset = dataset
        self.__request = request
        self.__prewhere_conditions = prewhere_conditions
        self.__final = final

    def format(self) -> str:
        """Generate a SQL string from the parameters."""
        body = self.__request.body
        source = self.__dataset \
            .get_dataset_schemas() \
            .get_read_schema() \
            .get_where_clause()

        aggregate_exprs = [util.column_expr(self.__dataset, col, body, alias, agg) for (agg, col, alias) in body['aggregations']]
        groupby = util.to_list(body['groupby'])
        group_exprs = [util.column_expr(self.__dataset, gb, body) for gb in groupby]
        selected_cols = [util.column_expr(self.__dataset, util.tuplify(colname), body) for colname in body.get('selected_columns', [])]
        select_clause = u'SELECT {}'.format(', '.join(group_exprs + aggregate_exprs + selected_cols))

        from_clause = u'FROM {}'.format(source)
        if self.__final:
            from_clause = u'{} FINAL'.format(from_clause)
        if 'sample' in body:
            from_clause = u'{} SAMPLE {}'.format(from_clause, body['sample'])

        join_clause = ''
        if 'arrayjoin' in body:
            join_clause = u'ARRAY JOIN {}'.format(body['arrayjoin'])

        where_clause = ''
        if body['conditions']:
            where_clause = u'WHERE {}'.format(util.conditions_expr(self.__dataset, body['conditions'], body))

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
        if body.get('orderby'):
            orderby = [util.column_expr(self.__dataset, util.tuplify(ob), body) for ob in util.to_list(body['orderby'])]
            orderby = [u'{} {}'.format(ob.lstrip('-'), 'DESC' if ob.startswith('-') else 'ASC') for ob in orderby]
            order_clause = u'ORDER BY {}'.format(', '.join(orderby))

        limitby_clause = ''
        if 'limitby' in body:
            limitby_clause = 'LIMIT {} BY {}'.format(*body['limitby'])

        limit_clause = ''
        if 'limit' in body:
            limit_clause = 'LIMIT {}, {}'.format(body.get('offset', 0), body['limit'])

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

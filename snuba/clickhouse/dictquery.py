from typing import Mapping

from snuba import settings as snuba_settings
from snuba import util
from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.datasets.dataset import Dataset
from snuba.query.columns import column_expr, conditions_expr
from snuba.query.parsing import ParsingContext
from snuba.request.request_settings import RequestSettings


class DictSqlQuery(SqlQuery):
    """
    SqlQuery implementation that transforms the legacy query representation
    based on the query body dictionary into the SQL string.
    The legacy query processing mixes processing and formatting together through the
    column_expr methods. This is the class that runs such process and such methods.

    To be used until the AST is not complete, then this class is supposed to be removed.
    """

    def __init__(
        self, dataset: Dataset, query: Query, settings: RequestSettings,
    ) -> None:
        parsing_context = ParsingContext()

        aggregate_exprs = [
            column_expr(dataset, col, query, parsing_context, alias, agg)
            for (agg, col, alias) in query.get_aggregations()
        ]
        groupby = util.to_list(query.get_groupby())
        group_exprs = [
            column_expr(dataset, gb, query, parsing_context) for gb in groupby
        ]
        column_names = query.get_selected_columns() or []
        selected_cols = [
            column_expr(dataset, util.tuplify(colname), query, parsing_context)
            for colname in column_names
        ]
        select_clause = "SELECT {}".format(
            ", ".join(group_exprs + aggregate_exprs + selected_cols)
        )

        from_clause = "FROM {}".format(query.get_data_source().format_from())

        if query.get_final():
            from_clause = "{} FINAL".format(from_clause)

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
            from_clause = "{} SAMPLE {}".format(from_clause, sample_rate)

        join_clause = ""
        if query.get_arrayjoin():
            join_clause = "ARRAY JOIN {}".format(query.get_arrayjoin())

        where_clause = ""
        if query.get_conditions():
            where_clause = "WHERE {}".format(
                conditions_expr(dataset, query.get_conditions(), query, parsing_context)
            )

        prewhere_clause = ""
        if query.get_prewhere():
            prewhere_clause = "PREWHERE {}".format(
                conditions_expr(dataset, query.get_prewhere(), query, parsing_context)
            )

        group_clause = ""
        if groupby:
            group_clause = "GROUP BY ({})".format(
                ", ".join(
                    column_expr(dataset, gb, query, parsing_context) for gb in groupby
                )
            )
            if query.has_totals():
                group_clause = "{} WITH TOTALS".format(group_clause)

        having_clause = ""
        having_conditions = query.get_having()
        if having_conditions:
            assert groupby, "found HAVING clause with no GROUP BY"
            having_clause = "HAVING {}".format(
                conditions_expr(dataset, having_conditions, query, parsing_context)
            )

        order_clause = ""
        if query.get_orderby():
            orderby = [
                column_expr(dataset, util.tuplify(ob), query, parsing_context)
                for ob in util.to_list(query.get_orderby())
            ]
            orderby = [
                "{} {}".format(ob.lstrip("-"), "DESC" if ob.startswith("-") else "ASC")
                for ob in orderby
            ]
            order_clause = "ORDER BY {}".format(", ".join(orderby))

        limitby_clause = ""
        if query.get_limitby() is not None:
            limitby_clause = "LIMIT {} BY {}".format(*query.get_limitby())

        limit_clause = ""
        if query.get_limit() is not None:
            limit_clause = "LIMIT {}, {}".format(query.get_offset(), query.get_limit())

        self.__sql_data = {
            k: v
            for k, v in [
                ("select", select_clause),
                ("from", from_clause),
                ("join", join_clause),
                ("prewhere", prewhere_clause),
                ("where", where_clause),
                ("group", group_clause),
                ("having", having_clause),
                ("order", order_clause),
                ("limitby", limitby_clause),
                ("limit", limit_clause),
            ]
            if v
        }

        self.__formatted_query = " ".join(
            [
                c
                for c in [
                    select_clause,
                    from_clause,
                    join_clause,
                    prewhere_clause,
                    where_clause,
                    group_clause,
                    having_clause,
                    order_clause,
                    limitby_clause,
                    limit_clause,
                ]
                if c
            ]
        )

    def _format_query_impl(self) -> str:
        return self.__formatted_query

    def sql_data(self) -> Mapping[str, str]:
        return self.__sql_data

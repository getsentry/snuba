from abc import ABC, abstractmethod
from typing import Optional

from snuba import util
from snuba.datasets.dataset import Dataset
from snuba.query.columns import column_expr, conditions_expr
from snuba.query.parsing import ParsingContext
from snuba.query.query import Query
from snuba.request.request_settings import RequestSettings


class ClickhouseQuery(ABC):
    """
    Generates and represents a Clickhouse query from a Request
    and a snuba Query
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
    def get_sampling_rate(self) -> Optional[float]:
        """
        This method probably won't exist when we will have a ClickHouse AST,
        since the format process will simply format the query to SQL without
        applying any specific logic.
        The method is used to ensure we do not tweak the the sampling rate provided
        by the query object while formatting since we need to return it
        reliably to the user.
        Or, if for some reason we have to tweak it, at least we have a way
        to return the value.
        """
        raise NotImplementedError


class DictClickhouseQuery(ClickhouseQuery):
    """
    Legacy Clickhouse query that transforms the Snuba Query based
    on the original query body dictionary into a string during construction
    without additional processing.

    To be used until the AST is not complete.
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

        self.__sample = query.get_sample()
        if self.__sample is not None:
            from_clause = "{} SAMPLE {}".format(from_clause, self.__sample)

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

    def get_sampling_rate(self) -> Optional[float]:
        return self.__sample

    def _format_query_impl(self) -> str:
        return self.__formatted_query

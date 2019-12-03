from typing import Optional

from snuba import settings
from snuba.clickhouse.query import ClickhouseQuery
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.query.parsing import ParsingContext
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
        self.__condition = query.get_condition_from_ast()
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
        self.__prewhere: query.get_prewhere_ast()
        self.__turbo = settings.get_turbo()
        self.__final = query.get_final()

        # Attributes that should be clickhouse specific that
        # have to be removed from the Snuba Query
        self.__sample = query.get_sample()
        self.__hastotals = query.has_totals()

        self.__settings = settings
        self.__formatted_query: Optional[str] = None

    def format_sql(self) -> str:
        if self.__formatted_query:
            return self.__formatted_query

        parsing_context = ParsingContext()

        selected_cols = [
            e.accept(ClickhouseExpressionFormatter(parsing_context))
            for e in self.__selected_columns
        ]
        columns = ", ".join(selected_cols)
        select_clause = f"SELECT {columns}"

        # TODO: The visitor approach will be used for the FROM clause as well.
        from_clause = f"FROM {self.__data_source.format_from()}"

        if self.__final:
            from_clause = f"{from_clause} FINAL"

        # TODO: Sampling rate will become one step of Clickhouse query processing
        if not self.__data_source.supports_sample():
            sample_rate = None
        else:
            if self.__sample:
                sample_rate = self.__sample
            elif self.__settings.get_turbo():
                sample_rate = settings.TURBO_SAMPLE_RATE
            else:
                sample_rate = None
        if sample_rate:
            from_clause = f"{from_clause} SAMPLE {sample_rate}"

        join_clause = ""
        if self.__arrayjoin:
            array_join = self.__arrayjoin.accept(
                ClickhouseExpressionFormatter(parsing_context)
            )
            join_clause = f"ARRAY JOIN {array_join}"

        where_clause = ""
        if self.__condition:
            formatted_condition = self.__condition.accept(
                ClickhouseExpressionFormatter(parsing_context)
            )
            where_clause = f"WHERE {formatted_condition}"

        # TODO: Pre where processing will become a step in Clickhouse Query processing
        # instead of being pulled from the Snuba Query
        prewhere_clause = ""
        if self.__prewhere:
            formatted_prewhere = self.__prewhere.accept(
                ClickhouseExpressionFormatter(parsing_context)
            )
            prewhere_clause = f"PREWHERE {formatted_prewhere}"

        group_clause = ""
        if self.__groupby:
            # reformat to use aliases generated during the select clause formatting.
            groupby_expressions = [
                e.accept(ClickhouseExpressionFormatter(parsing_context))
                for e in self.__groupby
            ]
            formatted_groupby = ", ".join(groupby_expressions)
            group_clause = f"GROUP BY ({formatted_groupby})"
            if self.__hastotals:
                group_clause = f"{group_clause} WITH TOTALS"

        having_clause = ""
        if self.__having:
            assert self.__groupby, "found HAVING clause with no GROUP BY"
            formatted_having = self.__having.accept(
                ClickhouseExpressionFormatter(parsing_context)
            )
            having_clause = f"HAVING {formatted_having}"

        order_clause = ""
        if self.__orderby:
            orderby = [
                f"{e.node.accept(ClickhouseExpressionFormatter(parsing_context))} {e.direction.value}"
                for e in self.__orderby
            ]
            formatted_orderby = ", ".join(orderby)
            order_clause = f"ORDER BY {formatted_orderby}"

        limitby_clause = ""
        if self.__limitby is not None:
            limitby_clause = "LIMIT {} BY {}".format(*self.__limitby)

        limit_clause = ""
        if self.__limit is not None:
            limit_clause = f"LIMIT {self.__offset}, {self.__limit}"

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

        return self.__formatted_query

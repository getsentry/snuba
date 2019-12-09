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

        if self.__having:
            assert self.__groupby, "found HAVING clause with no GROUP BY"

        # Clickhouse specific fields. Some are still in the Snuba
        # query and have to be moved.
        self.__turbo = settings.get_turbo()
        self.__final = query.get_final()
        self.__sample = query.get_sample()
        self.__hastotals = query.has_totals()
        # TODO: Pre where processing will become a step in Clickhouse Query processing
        # instead of being pulled from the Snuba Query
        self.__prewhere = query.get_prewhere_ast()

        self.__settings = settings
        self.__formatted_query: Optional[str] = None

    def format_sql(self) -> str:
        if self.__formatted_query:
            return self.__formatted_query

        parsing_context = ParsingContext()
        formatter = ClickhouseExpressionFormatter(parsing_context)

        selected_cols = [e.accept(formatter) for e in self.__selected_columns]
        select_clause = f"SELECT {', '.join(selected_cols)}"

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

        array_join_clause = ""
        if self.__arrayjoin:
            formatted_array_join = self.__arrayjoin.accept(formatter)
            array_join_clause = f"ARRAY JOIN {formatted_array_join}"

        prewhere_clause = ""
        if self.__prewhere:
            formatted_prewhere = self.__prewhere.accept(formatter)
            prewhere_clause = f"PREWHERE {formatted_prewhere}"

        where_clause = ""
        if self.__condition:
            formatted_condition = self.__condition.accept(formatter)
            where_clause = f"WHERE {formatted_condition}"

        group_clause = ""
        if self.__groupby:
            # reformat to use aliases generated during the select clause formatting.
            groupby_expressions = [e.accept(formatter) for e in self.__groupby]
            group_clause = f"GROUP BY ({', '.join(groupby_expressions)})"
            if self.__hastotals:
                group_clause = f"{group_clause} WITH TOTALS"

        having_clause = ""
        if self.__having:
            having_clause = f"HAVING {self.__having.accept(formatter)}"

        order_clause = ""
        if self.__orderby:
            orderby = [
                f"{e.node.accept(formatter)} {e.direction.value}"
                for e in self.__orderby
            ]
            order_clause = f"ORDER BY {', '.join(orderby)}"

        limitby_clause = ""
        if self.__limitby is not None:
            limitby_clause = "LIMIT {} BY {}".format(*self.__limitby)

        limit_clause = ""
        if self.__limit is not None:
            limit_clause = f"LIMIT {self.__limit} OFFSET {self.__offset}"

        self.__formatted_query = " ".join(
            [
                c
                for c in [
                    select_clause,
                    from_clause,
                    array_join_clause,
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

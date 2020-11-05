from typing import Mapping, Optional, Sequence, Tuple

from snuba import settings
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.clickhouse.sql import SqlQuery
from snuba.query.parsing import ParsingContext
from snuba.request.request_settings import RequestSettings


class AstSqlQuery(SqlQuery):
    """
    SqlQuery implementation that builds the SQL query out of the
    AST representation present in the Clickhouse Query object.
    """

    def __init__(self, query: Query, settings: RequestSettings,) -> None:
        # Clickhouse query structure
        # Referencing them here directly since it makes it easier
        # to process this query independently from the Clickhouse Query
        # and there is no risk in doing so since they are immutable.
        self.__selected_columns = query.get_selected_columns_from_ast()
        self.__condition = query.get_condition_from_ast()
        self.__groupby = query.get_groupby_from_ast()
        self.__having = query.get_having_from_ast()
        self.__orderby = query.get_orderby_from_ast()
        self.__from_clause = query.get_from_clause()
        self.__arrayjoin = query.get_arrayjoin_from_ast()
        self.__granularity = query.get_granularity()
        self.__limit = query.get_limit()
        self.__limitby = query.get_limitby()
        self.__offset = query.get_offset()

        if self.__having:
            assert self.__groupby, "found HAVING clause with no GROUP BY"

        self.__turbo = settings.get_turbo()
        self.__final = query.get_final()
        self.__sample = query.get_sample()
        self.__hastotals = query.has_totals()
        self.__prewhere = query.get_prewhere_ast()

        self.__settings = settings
        self.__sql_data_list: Optional[Sequence[Tuple[str, str]]] = None
        self.__formatted_query: Optional[str] = None
        self.__sql_data: Optional[Mapping[str, str]] = None

    def _sql_data_list(self) -> Sequence[Tuple[str, str]]:
        if self.__sql_data_list:
            return self.__sql_data_list

        parsing_context = ParsingContext()
        formatter = ClickhouseExpressionFormatter(parsing_context)

        selected_cols = [
            e.expression.accept(formatter) for e in self.__selected_columns
        ]
        select_clause = f"SELECT {', '.join(selected_cols)}"

        # TODO: The visitor approach will be used for the FROM clause as well.
        from_clause = f"FROM {self.__from_clause.format_from()}"

        if self.__final:
            from_clause = f"{from_clause} FINAL"

        # TODO: Sampling rate will become one step of Clickhouse query processing
        if not self.__from_clause.supports_sample():
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
            where_clause = f"WHERE {self.__condition.accept(formatter)}"

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
                f"{e.expression.accept(formatter)} {e.direction.value}"
                for e in self.__orderby
            ]
            order_clause = f"ORDER BY {', '.join(orderby)}"

        limitby_clause = ""
        if self.__limitby is not None:
            limitby_clause = "LIMIT {} BY {}".format(*self.__limitby)

        limit_clause = ""
        if self.__limit is not None:
            limit_clause = f"LIMIT {self.__limit} OFFSET {self.__offset}"

        self.__sql_data_list = [
            (k, v)
            for k, v in [
                ("select", select_clause),
                ("from", from_clause),
                ("array_join", array_join_clause),
                ("prewhere", prewhere_clause),
                ("where", where_clause),
                ("group", group_clause),
                ("having", having_clause),
                ("order", order_clause),
                ("limitby", limitby_clause),
                ("limit", limit_clause),
            ]
            if v
        ]

        return self.__sql_data_list

    def format_sql(self, format: Optional[str] = None) -> str:
        if self.__formatted_query is None:
            query = " ".join([c for _, c in self._sql_data_list()])
            self.__formatted_query = query
        else:
            query = self.__formatted_query

        if format is not None:
            query = f"{query} FORMAT {format}"

        return query

    def sql_data(self) -> Mapping[str, str]:
        if self.__sql_data:
            return self.__sql_data

        self.__sql_data = dict(self._sql_data_list())

        return self.__sql_data

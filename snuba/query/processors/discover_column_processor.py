from typing import Callable, Mapping, Optional

from snuba.clickhouse.columns import ColumnSet
from snuba.query.dsl import multiply
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.request.request_settings import RequestSettings


EVENTS = "events"
TRANSACTIONS = "transactions"


class DiscoverColumnProcessor(QueryProcessor):
    """
    Translate the time group columns of the dataset to use the correct granularity rounding.
    """

    def __init__(
        self,
        events_columns: ColumnSet,
        transactions_columns: ColumnSet,
        detect_table_fn: Callable[[Query, ColumnSet, ColumnSet], str],
    ) -> None:
        self.detect_table = detect_table_fn
        self.__events_columns = events_columns
        self.__transactions_columns = transactions_columns

    def get_new_column_name(self, detected_dataset: str, column_name: str) -> str:
        if detected_dataset == TRANSACTIONS:
            if column_name == "time":
                return "finish_ts"
            if column_name == "type":
                return "'transaction'"
            if column_name == "timestamp":
                return "finish_ts"
            if column_name == "username":
                return "user_name"
            if column_name == "email":
                return "user_email"
            if column_name == "transaction":
                return "transaction_name"
            if column_name == "message":
                return "transaction_name"
            if column_name == "title":
                return "transaction_name"
            if column_name == "group_id":
                # TODO: We return 0 here instead of NULL so conditions like group_id
                # in (1, 2, 3) will work, since Clickhouse won't run a query like:
                # SELECT (NULL AS group_id) FROM transactions WHERE group_id IN (1, 2, 3)
                # When we have the query AST, we should solve this by transforming the
                # nonsensical conditions instead.
                return "0"
            if column_name == "geo_country_code":
                column_name = "contexts[geo.country_code]"
            if column_name == "geo_region":
                column_name = "contexts[geo.region]"
            if column_name == "geo_city":
                column_name = "contexts[geo.city]"
            if self.__events_columns.get(column_name):
                return "NULL"
        else:
            if column_name == "time":
                return "timestamp"
            if column_name == "release":
                column_name = "tags[sentry:release]"
            if column_name == "dist":
                column_name = "tags[sentry:dist]"
            if column_name == "user":
                column_name = "tags[sentry:user]"
            if self.__transactions_columns.get(column_name):
                return "NULL"

        return column_name

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                detected_dataset = self.detect_table(
                    query, self.__events_columns, self.__transactions_columns
                )
                column_name = exp.column_name
                new_column_name = self.get_new_column_name(
                    detected_dataset, column_name
                )
                return Column(exp.alias, new_column_name, exp.table_name)

            return exp

        query.transform_expressions(process_column)

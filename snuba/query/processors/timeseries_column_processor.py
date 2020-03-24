from typing import Mapping

from snuba.query.expressions import (
    Expression,
    Column,
)
from snuba.query.query import Query
from snuba.query.query_processor import QueryProcessor
from snuba.query.processors.performance_expressions import apdex
from snuba.request.request_settings import RequestSettings


class TimeSeriesColumnProcessor(QueryProcessor):
    """
    Mimics the old column_expr functionality
    """

    def __init__(self, time_columns: Mapping[str, str]) -> None:
        self.time_columns = time_columns

    def time_expr(self, column_name: str, granularity: int) -> str:
        template = {
            3600: "toStartOfHour({column})",
            60: "toStartOfMinute({column})",
            86400: "toDate({column})",
        }.get(
            granularity,
            "toDateTime(intDiv(toUInt32({column}), {granularity}) * {granularity})",
        )
        return template.format(column=column_name, granularity=granularity)

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name in self.time_columns:
                    real_column = self.time_columns[exp.column_name]
                    time_column = self.time_expr(real_column, query.get_granularity())
                    return Column(exp.alias, time_column, exp.table_name)
                    
            
            return exp

        query.transform_expressions(process_column)

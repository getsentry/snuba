from typing import Mapping, Optional

from snuba.query.dsl import multiply
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class TimeSeriesColumnProcessor(QueryProcessor):
    """
    Translate the time group columns of the dataset to use the correct granularity rounding.
    """

    def __init__(self, time_columns: Mapping[str, str]) -> None:
        self.__time_columns = time_columns

    def time_expr(
        self, column_name: str, granularity: int, alias: Optional[str]
    ) -> str:
        function_call = {
            3600: FunctionCall(
                alias,
                "toStartOfHour",
                (Column(None, None, column_name), Literal(None, "Universal")),
            ),
            60: FunctionCall(
                alias,
                "toStartOfMinute",
                (Column(None, None, column_name), Literal(None, "Universal")),
            ),
            86400: FunctionCall(
                alias,
                "toDate",
                (Column(None, None, column_name), Literal(None, "Universal")),
            ),
        }.get(granularity)
        if not function_call:
            function_call = FunctionCall(
                alias,
                "toDateTime",
                (
                    multiply(
                        FunctionCall(
                            None,
                            "intDiv",
                            (
                                FunctionCall(
                                    None,
                                    "toUInt32",
                                    (Column(None, None, column_name),),
                                ),
                                Literal(None, granularity),
                            ),
                        ),
                        Literal(None, granularity),
                    ),
                    Literal(None, "Universal"),
                ),
            )

        return function_call

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name in self.__time_columns:
                    real_column_name = self.__time_columns[exp.column_name]
                    granularity = query.get_granularity()
                    if granularity is None:
                        granularity = 3600
                    time_column_fn = self.time_expr(
                        real_column_name, granularity, exp.alias
                    )
                    return time_column_fn

            return exp

        query.transform_expressions(process_column)

from typing import Mapping, Optional, Sequence

from snuba.query.conditions import ConditionFunctions
from snuba.query.dsl import multiply
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.matchers import (
    Any,
    Column as ColumnMatch,
    FunctionCall as FunctionCallMatch,
    Literal as LiteralMatch,
    Or,
    Param,
    String,
)
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings
from snuba.util import parse_datetime


class TimeSeriesProcessor(QueryProcessor):
    """
    Firstly, translate the time group columns of the dataset to use the correct columns and granularity rounding.
    Secondly, if there are conditions on the defined parse columns, change the literal to be a datetime instead of a string.
    """

    def __init__(
        self, time_group_columns: Mapping[str, str], time_parse_columns: Sequence[str]
    ) -> None:
        # Column names that should be mapped to different columns.
        self.__time_replace_columns = time_group_columns

        # Columns names that should be grouped based on the query granularity
        self.__time_grouped_columns = set(time_group_columns.values())

        # time_parse_columns is a list of columns that, if used in a condition, should be compared with datetimes.
        # TODO: This handles only the simplest case, what happens if the column is wrapped in a function?
        self.condition_match = FunctionCallMatch(
            Or(
                [
                    String(ConditionFunctions.GT),
                    String(ConditionFunctions.GTE),
                    String(ConditionFunctions.LT),
                    String(ConditionFunctions.LTE),
                    String(ConditionFunctions.EQ),
                    String(ConditionFunctions.NEQ),
                ]
            ),
            (
                ColumnMatch(
                    None,
                    Param(
                        "column_name", Or([String(tc) for tc in time_parse_columns]),
                    ),
                ),
                Param("literal", LiteralMatch(Any(str))),
            ),
        )

    def __replace_time_column(self, exp: Expression) -> Expression:
        if isinstance(exp, Column):
            if exp.column_name in self.__time_replace_columns:
                real_column_name = self.__time_replace_columns[exp.column_name]
                return Column(exp.alias, exp.table_name, real_column_name)
        return exp

    def __group_time_column(
        self, exp: Expression, granularity: Optional[int]
    ) -> Expression:
        if isinstance(exp, Column):
            if exp.column_name in self.__time_grouped_columns:
                if granularity is None:
                    granularity = 3600
                time_column_fn = self.__group_time_function(
                    exp.column_name, granularity, exp.alias
                )
                return time_column_fn

        return exp

    def __group_time_function(
        self, column_name: str, granularity: int, alias: Optional[str]
    ) -> FunctionCall:
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

    def __process_condition(self, exp: Expression) -> Expression:
        result = self.condition_match.match(exp)
        if result is not None:
            literal = result.expression("literal")
            assert isinstance(exp, FunctionCall)  # mypy
            assert isinstance(literal, Literal)  # mypy
            try:
                value = parse_datetime(str(literal.value))
            except ValueError as err:
                column_name = result.string("column_name")
                raise InvalidQueryException(
                    f"Illegal datetime in condition on column {column_name}: '{literal.value}''"
                ) from err

            return FunctionCall(
                exp.alias,
                exp.function_name,
                (exp.parameters[0], Literal(literal.alias, value),),
            )

        return exp

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # Process the query in 3 steps:
        # 1) Use time_group_columns to map the old column names to the new column names
        query.transform_expressions(self.__replace_time_column)

        # 2) Now that all the column names are correct, search the conditions for time based
        # conditions and make sure the comparison is with a datetime
        condition = query.get_condition_from_ast()
        if condition:
            query.set_ast_condition(condition.transform(self.__process_condition))

        # 3) Map the time_group_columns into functions based on the query granularity.
        granularity = query.get_granularity()
        query.transform_expressions(
            lambda exp: self.__group_time_column(exp, granularity)
        )

from typing import Mapping, Optional, Sequence

from snuba.query.conditions import ConditionFunctions
from snuba.query.dsl import multiply
from snuba.query.exceptions import InvalidQueryException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.matchers import (
    Any,
    AnyExpression,
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
        self.__time_group_columns = time_group_columns
        self.__time_parse_columns = time_parse_columns
        self.condition_match = FunctionCallMatch(
            None,
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
                Or(
                    [
                        ColumnMatch(
                            None,
                            None,
                            Param(
                                "column_name",
                                Or([String(tc) for tc in self.__time_parse_columns]),
                            ),
                        ),
                        FunctionCallMatch(
                            None,
                            None,
                            (
                                ColumnMatch(
                                    None,
                                    None,
                                    Param(
                                        "column_name",
                                        Or(
                                            [
                                                String(tc)
                                                for tc in self.__time_parse_columns
                                            ]
                                        ),
                                    ),
                                ),
                                AnyExpression(),
                            ),
                        ),
                    ]
                ),
                Param("literal", LiteralMatch(None, Any(str))),
            ),
        )

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

    def process_condition(self, exp: Expression) -> Expression:
        result = self.condition_match.match(exp)
        if result is not None:
            literal = result.expression("literal")
            assert isinstance(exp, FunctionCall)  # mypy
            assert isinstance(literal, Literal)  # mypy
            try:
                value = parse_datetime(literal.value)
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
        def process_column(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name in self.__time_group_columns:
                    real_column_name = self.__time_group_columns[exp.column_name]
                    granularity = query.get_granularity()
                    if granularity is None:
                        granularity = 3600
                    time_column_fn = self.time_expr(
                        real_column_name, granularity, exp.alias
                    )
                    return time_column_fn

            return exp

        query.transform_expressions(process_column)

        condition = query.get_condition_from_ast()
        if condition:
            query.set_ast_condition(condition.transform(self.process_condition))

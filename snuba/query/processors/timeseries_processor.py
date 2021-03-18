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

        # time_parse_columns is a list of columns that, if used in a condition, should be compared with datetimes.
        # The columns here might overlap with the columns that get replaced, so we have to search for transformed
        # columns.
        column_match = ColumnMatch(
            None, Param("column_name", Or([String(tc) for tc in time_parse_columns]),),
        )
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
                Or(
                    [
                        column_match,
                        FunctionCallMatch(
                            Or(
                                [
                                    String("toStartOfHour"),
                                    String("toStartOfMinute"),
                                    String("toStartOfDay"),
                                    String("toDate"),
                                ]
                            ),
                            (column_match,),
                            with_optionals=True,
                        ),
                        FunctionCallMatch(
                            String("toDateTime"),
                            (
                                FunctionCallMatch(
                                    String("multiply"),
                                    (
                                        FunctionCallMatch(
                                            String("intDiv"),
                                            (
                                                FunctionCallMatch(
                                                    String("toUInt32"), (column_match,),
                                                ),
                                                LiteralMatch(Any(int)),
                                            ),
                                        ),
                                        LiteralMatch(Any(int)),
                                    ),
                                ),
                                LiteralMatch(Any(str)),
                            ),
                        ),
                    ]
                ),
                Param("literal", LiteralMatch(Any(str))),
            ),
        )

    def __group_time_column(
        self, exp: Expression, granularity: Optional[int]
    ) -> Expression:
        if isinstance(exp, Column):
            if exp.column_name in self.__time_replace_columns:
                real_column_name = self.__time_replace_columns[exp.column_name]
                if granularity is None:
                    granularity = 3600
                time_column_fn = self.__group_time_function(
                    real_column_name, granularity, exp.alias
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
                (exp.parameters[0], Literal(literal.alias, value)),
            )

        return exp

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # Use time_group_columns to map the old column names to the new column names, and
        # map the time_group_columns into functions based on the query granularity.
        granularity = query.get_granularity()
        query.transform_expressions(
            lambda exp: self.__group_time_column(exp, granularity)
        )

        # Now that all the column names are correct, search the conditions for time based
        # conditions and make sure the comparison is with a datetime
        condition = query.get_condition()
        if condition:
            query.set_ast_condition(condition.transform(self.__process_condition))


GRANULARITY_MAPPING = {
    "toStartOfMinute": 60,
    "toStartOfHour": 3600,
    "toDate": 86400,
}


def extract_granularity_from_query(query: Query, column: str) -> Optional[int]:
    """
    This extracts the `granularity` from the `groupby` statement of the query.
    The matches are essentially the reverse of `TimeSeriesProcessor.__group_time_function`.
    """
    groupby = query.get_groupby()

    column_match = ColumnMatch(None, String(column))
    fn_match = FunctionCallMatch(
        Param(
            "time_fn",
            Or(
                [
                    String("toStartOfHour"),
                    String("toStartOfMinute"),
                    String("toStartOfDay"),
                    String("toDate"),
                ]
            ),
        ),
        (column_match,),
        with_optionals=True,
    )
    expr_match = FunctionCallMatch(
        String("toDateTime"),
        (
            FunctionCallMatch(
                String("multiply"),
                (
                    FunctionCallMatch(
                        String("intDiv"),
                        (
                            FunctionCallMatch(String("toUInt32"), (column_match,)),
                            LiteralMatch(Param("granularity", Any(int))),
                        ),
                    ),
                    LiteralMatch(Param("granularity", Any(int))),
                ),
            ),
            LiteralMatch(Any(str)),
        ),
    )

    for top_expr in groupby:
        for expr in top_expr:
            result = fn_match.match(expr)
            if result is not None:
                return GRANULARITY_MAPPING[result.string("time_fn")]

            result = expr_match.match(expr)
            if result is not None:
                return result.integer("granularity")

    return None

from typing import Set

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import ConditionFunctions, FUNCTION_TO_OPERATOR
from snuba.query.exceptions import ValidationException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.matchers import Or, Param, String
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.request.request_settings import RequestSettings


class HexIntValidationError(ValidationException):
    pass


class HexIntColumnProcessor(QueryProcessor):
    def __init__(self, columns: Set[str]):
        self.__columns = columns
        column_match = Or([String(col) for col in columns])

        literal = Param("literal", LiteralMatch(AnyMatch(str)))

        operator = Param(
            "operator",
            Or(
                [
                    String(op)
                    for op in FUNCTION_TO_OPERATOR
                    if op not in (ConditionFunctions.IN, ConditionFunctions.NOT_IN)
                ]
            ),
        )

        in_operators = Param(
            "operator",
            Or((String(ConditionFunctions.IN), String(ConditionFunctions.NOT_IN))),
        )

        col = Param("col", ColumnMatch(None, column_match))

        self.__condition_matcher = Or(
            [
                FunctionCallMatch(operator, (literal, col)),
                FunctionCallMatch(operator, (col, literal)),
            ]
        )

        self.__in_condition_matcher = FunctionCallMatch(
            in_operators,
            (col, Param("tuple", FunctionCallMatch(String("tuple"), None)),),
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def translate_hexint_literal(exp: Expression) -> Expression:
            try:
                assert isinstance(exp, Literal)
                assert isinstance(exp.value, str)
                return Literal(alias=exp.alias, value=int(exp.value, 16))
            except AssertionError:
                raise HexIntValidationError("Invalid hexint")

        def strip_column_alias(exp: Expression) -> Expression:
            assert isinstance(exp, Column)
            return Column(
                alias=None, table_name=exp.table_name, column_name=exp.column_name
            )

        def process_condition(exp: Expression) -> Expression:
            match = self.__condition_matcher.match(exp)
            if match:
                return FunctionCall(
                    exp.alias,
                    match.string("operator"),
                    (
                        strip_column_alias(match.expression("col")),
                        translate_hexint_literal(match.expression("literal")),
                    ),
                )

            in_condition_match = self.__in_condition_matcher.match(exp)

            if in_condition_match:
                tuple_func = in_condition_match.expression("tuple")
                assert isinstance(tuple_func, FunctionCall)
                new_tuple_func = FunctionCall(
                    tuple_func.alias,
                    tuple_func.function_name,
                    parameters=tuple(
                        [translate_hexint_literal(lit) for lit in tuple_func.parameters]
                    ),
                )
                return FunctionCall(
                    exp.alias,
                    in_condition_match.string("operator"),
                    (
                        strip_column_alias(in_condition_match.expression("col")),
                        new_tuple_func,
                    ),
                )

            return exp

        def process_all(exp: Expression) -> Expression:
            if isinstance(exp, Column) and exp.column_name in self.__columns:
                return FunctionCall(
                    exp.alias,
                    "lower",
                    (
                        FunctionCall(
                            None, "hex", (Column(None, None, exp.column_name),),
                        ),
                    ),
                )

            return exp

        query.transform_expressions(process_all, skip_transform_condition=True)

        condition = query.get_condition()
        if condition:
            query.set_ast_condition(condition.transform(process_condition))

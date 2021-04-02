from abc import ABC, abstractmethod
from typing import Set

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import FUNCTION_TO_OPERATOR, ConditionFunctions
from snuba.query.exceptions import ValidationException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or, Param, String
from snuba.request.request_settings import RequestSettings


class ColumnTypeError(ValidationException):
    pass


class BaseTypeConverter(QueryProcessor, ABC):
    def __init__(self, columns: Set[str]):
        self.columns = columns
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
            (
                col,
                Param(
                    "tuple",
                    FunctionCallMatch(String("tuple"), all_parameters=LiteralMatch()),
                ),
            ),
        )

    def strip_column_alias(self, exp: Expression) -> Expression:
        assert isinstance(exp, Column)
        return Column(
            alias=None, table_name=exp.table_name, column_name=exp.column_name
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        query.transform_expressions(
            self.process_expressions, skip_transform_condition=True
        )

        condition = query.get_condition()
        if condition:
            processed = condition.transform(self.process_optimizable_condition)
            if processed == condition:
                processed = condition.transform(self.process_expressions)

            query.set_ast_condition(processed)

    def process_optimizable_condition(self, exp: Expression) -> Expression:
        def assert_literal(lit: Expression) -> Literal:
            assert isinstance(lit, Literal)
            return lit

        match = self.__condition_matcher.match(exp)
        if match:
            return FunctionCall(
                exp.alias,
                match.string("operator"),
                (
                    self.strip_column_alias(match.expression("col")),
                    self.translate_literal(assert_literal(match.expression("literal"))),
                ),
            )

        in_condition_match = self.__in_condition_matcher.match(exp)

        if in_condition_match:
            tuple_func = in_condition_match.expression("tuple")
            assert isinstance(tuple_func, FunctionCall)

            params = tuple_func.parameters
            for param in params:
                assert isinstance(param, Literal)

            new_tuple_func = FunctionCall(
                tuple_func.alias,
                tuple_func.function_name,
                parameters=tuple(
                    [
                        self.translate_literal(assert_literal(lit))
                        for lit in tuple_func.parameters
                    ]
                ),
            )
            return FunctionCall(
                exp.alias,
                in_condition_match.string("operator"),
                (
                    self.strip_column_alias(in_condition_match.expression("col")),
                    new_tuple_func,
                ),
            )

        return exp

    @abstractmethod
    def translate_literal(self, exp: Literal) -> Literal:
        raise NotImplementedError

    @abstractmethod
    def process_expressions(self, exp: Expression) -> Expression:
        raise NotImplementedError

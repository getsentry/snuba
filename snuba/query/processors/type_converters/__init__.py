from abc import ABC, abstractmethod
from typing import Set

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import ConditionFunctions
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
                    for op in (
                        ConditionFunctions.EQ,
                        ConditionFunctions.NEQ,
                        ConditionFunctions.IS_NULL,
                        ConditionFunctions.IS_NOT_NULL,
                    )
                ]
            ),
        )

        unoptimizable_operator = Param(
            "operator",
            Or(
                [
                    String(op)
                    for op in (
                        ConditionFunctions.GT,
                        ConditionFunctions.GTE,
                        ConditionFunctions.LT,
                        ConditionFunctions.LTE,
                        ConditionFunctions.LIKE,
                        ConditionFunctions.NOT_LIKE,
                    )
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
                FunctionCallMatch(Param("operator", String("has")), (col, literal)),
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

        self.__unoptimizable_condition_matcher = Or(
            [
                FunctionCallMatch(unoptimizable_operator, (literal, col)),
                FunctionCallMatch(unoptimizable_operator, (col, literal)),
            ]
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        query.transform_expressions(
            self._process_expressions, skip_transform_condition=True
        )

        condition = query.get_condition()
        if condition is not None:
            if self.__is_optimizable_expression(condition):
                processed = condition.transform(self.__process_optimizable_condition)
            else:
                processed = condition.transform(self._process_expressions)

            query.set_ast_condition(processed)

    def __strip_column_alias(self, exp: Expression) -> Expression:
        assert isinstance(exp, Column)
        return Column(
            alias=None, table_name=exp.table_name, column_name=exp.column_name
        )

    def __is_optimizable_expression(self, exp: Expression) -> bool:
        """
        Returns true if the entire expression can optimized, otherwise false.
        """
        for e in exp:
            match = self.__unoptimizable_condition_matcher.match(e)
            if match is not None:
                return False

        return True

    def __process_optimizable_condition(self, exp: Expression) -> Expression:
        def assert_literal(lit: Expression) -> Literal:
            assert isinstance(lit, Literal)
            return lit

        match = self.__condition_matcher.match(exp)
        if match is not None:
            return FunctionCall(
                exp.alias,
                match.string("operator"),
                (
                    self.__strip_column_alias(match.expression("col")),
                    self._translate_literal(
                        assert_literal(match.expression("literal"))
                    ),
                ),
            )

        in_condition_match = self.__in_condition_matcher.match(exp)

        if in_condition_match is not None:
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
                        self._translate_literal(assert_literal(lit))
                        for lit in tuple_func.parameters
                    ]
                ),
            )
            return FunctionCall(
                exp.alias,
                in_condition_match.string("operator"),
                (
                    self.__strip_column_alias(in_condition_match.expression("col")),
                    new_tuple_func,
                ),
            )

        return exp

    @abstractmethod
    def _translate_literal(self, exp: Literal) -> Expression:
        raise NotImplementedError

    @abstractmethod
    def _process_expressions(self, exp: Expression) -> Expression:
        raise NotImplementedError

from abc import ABC, abstractmethod
from typing import Set

from snuba.clickhouse.query import Query
from snuba.query.conditions import ConditionFunctions
from snuba.query.exceptions import ValidationException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.matchers import Or, Param, String
from snuba.query.processors.physical import ClickhouseQueryProcessor
from snuba.query.query_settings import QuerySettings


class ColumnTypeError(ValidationException):
    pass


class BaseTypeConverter(ClickhouseQueryProcessor, ABC):
    def __init__(self, columns: Set[str], optimize_ordering: bool = False):
        self.columns = columns
        self.optimize_ordering = optimize_ordering
        column_match = Or([String(col) for col in columns])

        literal = Param("literal", LiteralMatch(AnyMatch(str)))

        ordering_operators = (
            ConditionFunctions.GT,
            ConditionFunctions.GTE,
            ConditionFunctions.LT,
            ConditionFunctions.LTE,
        )

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
                        *(ordering_operators if self.optimize_ordering else ()),
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
                        ConditionFunctions.LIKE,
                        ConditionFunctions.NOT_LIKE,
                        *(() if self.optimize_ordering else ordering_operators),
                    )
                ]
            ),
        )

        in_operators = Param(
            "operator",
            Or((String(ConditionFunctions.IN), String(ConditionFunctions.NOT_IN))),
        )

        col = Param("col", ColumnMatch(None, column_match))

        casted_col = Param(
            "casted_col",
            FunctionCallMatch(Param("cast", String("cast")), (col, LiteralMatch(AnyMatch(str)))),
        )

        self.__condition_matcher = Or(
            [
                FunctionCallMatch(operator, (literal, col)),
                FunctionCallMatch(operator, (col, literal)),
                FunctionCallMatch(Param("operator", String("has")), (col, literal)),
            ]
        )

        self.__casted_condition_matcher = Or(
            [
                FunctionCallMatch(operator, (casted_col, literal)),
                FunctionCallMatch(operator, (literal, casted_col)),
                FunctionCallMatch(Param("operator", String("has")), (casted_col, literal)),
            ]
        )

        self.__in_condition_matcher = FunctionCallMatch(
            in_operators,
            (
                col,
                Param(
                    "literals",
                    FunctionCallMatch(
                        Or([String("array"), String("tuple")]),
                        all_parameters=LiteralMatch(),
                    ),
                ),
            ),
        )

        self.__unoptimizable_condition_matcher = Or(
            [
                FunctionCallMatch(unoptimizable_operator, (literal, col)),
                FunctionCallMatch(unoptimizable_operator, (col, literal)),
            ]
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        query.transform_expressions(self._process_expressions, skip_transform_condition=True)

        condition = query.get_condition()
        if condition is not None:
            if self.__contains_unoptimizable_condition(condition):
                processed = condition.transform(self._process_expressions)
            else:
                processed = condition.transform(self.__process_optimizable_condition)
                if condition == processed:
                    processed = processed.transform(self._process_expressions)

            query.set_ast_condition(processed)

    def __strip_column_alias(self, exp: Expression) -> Expression:
        assert isinstance(exp, Column)
        return Column(alias=None, table_name=exp.table_name, column_name=exp.column_name)

    def __contains_unoptimizable_condition(self, exp: Expression) -> bool:
        """
        Returns true if there is an unoptimizable condition, otherwise false.
        """
        for e in exp:
            match = self.__unoptimizable_condition_matcher.match(e)
            if match is not None:
                return True

        return False

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
                    self._translate_literal(assert_literal(match.expression("literal"))),
                ),
            )

        casted_match = self.__casted_condition_matcher.match(exp)
        if casted_match is not None:
            return FunctionCall(
                exp.alias,
                casted_match.string("operator"),
                (
                    self.__strip_column_alias(casted_match.expression("col")),
                    self._translate_literal(assert_literal(casted_match.expression("literal"))),
                ),
            )

        in_condition_match = self.__in_condition_matcher.match(exp)

        if in_condition_match is not None:
            collection_func = in_condition_match.expression("literals")
            assert isinstance(collection_func, FunctionCall)

            params = collection_func.parameters
            for param in params:
                assert isinstance(param, Literal)

            wrapper = tuple if collection_func.function_name == "tuple" else list
            new_collection_func = FunctionCall(
                collection_func.alias,
                collection_func.function_name,
                parameters=wrapper(
                    [
                        self._translate_literal(assert_literal(lit))
                        for lit in collection_func.parameters
                    ]
                ),
            )
            return FunctionCall(
                exp.alias,
                in_condition_match.string("operator"),
                (
                    self.__strip_column_alias(in_condition_match.expression("col")),
                    new_collection_func,
                ),
            )

        return exp

    @abstractmethod
    def _translate_literal(self, exp: Literal) -> Expression:
        raise NotImplementedError

    @abstractmethod
    def _process_expressions(self, exp: Expression) -> Expression:
        raise NotImplementedError

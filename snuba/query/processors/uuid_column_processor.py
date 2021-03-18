from typing import Set
import uuid

from snuba import environment
from snuba.query.conditions import (
    ConditionFunctions,
    FUNCTION_TO_OPERATOR,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.exceptions import ValidationException
from snuba.clickhouse.query import Query
from snuba.query.matchers import (
    Or,
    Param,
    String,
)
from snuba.query.matchers import Any as AnyMatch
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.clickhouse.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings
from snuba.utils.metrics.wrapper import MetricsWrapper


metrics = MetricsWrapper(environment.metrics, "api.query.uuid_processor")


class UUIDTranslationError(ValidationException):
    pass


class UUIDColumnProcessor(QueryProcessor):
    """
    Processor that handles columns which are stored as UUID in ClickHouse but
    are represented externally as a non hyphenated hex string.
    """

    def __init__(self, uuid_columns: Set[str]) -> None:
        self.__uuid_columns = uuid_columns
        uuid_column_match = Or([String(col) for col in uuid_columns])

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

        col_func = FunctionCallMatch(
            String("replaceAll"),
            (
                FunctionCallMatch(
                    String("toString"),
                    (Param("col", ColumnMatch(None, uuid_column_match)),),
                ),
            ),
            with_optionals=True,
        )

        literal = Param("literal", LiteralMatch(AnyMatch(str)))

        self.__condition_matcher = Or(
            [
                FunctionCallMatch(operator, (literal, col_func)),
                FunctionCallMatch(operator, (col_func, literal)),
            ]
        )

        self.__in_condition_matcher = FunctionCallMatch(
            in_operators,
            (col_func, Param("tuple", FunctionCallMatch(String("tuple"), None)),),
        )

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def translate_uuid_literal(exp: Expression) -> Expression:
            try:
                assert isinstance(exp, Literal)
                new_val = str(uuid.UUID(str(exp.value)))
                return Literal(alias=exp.alias, value=new_val)
            except (AssertionError, ValueError):
                raise UUIDTranslationError("Not a valid UUID string")

        def process_condition(exp: Expression) -> Expression:
            match = self.__condition_matcher.match(exp)

            if match:
                return FunctionCall(
                    exp.alias,
                    match.string("operator"),
                    (
                        match.expression("col"),
                        translate_uuid_literal(match.expression("literal")),
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
                        [translate_uuid_literal(lit) for lit in tuple_func.parameters]
                    ),
                )
                return FunctionCall(
                    exp.alias,
                    in_condition_match.string("operator"),
                    (in_condition_match.expression("col"), new_tuple_func,),
                )

            return exp

        def process_all(exp: Expression) -> Expression:
            if isinstance(exp, Column):
                if exp.column_name in self.__uuid_columns:
                    return FunctionCall(
                        exp.alias,
                        "replaceAll",
                        (
                            FunctionCall(
                                None,
                                "toString",
                                (Column(None, None, exp.column_name),),
                            ),
                            Literal(None, "-"),
                            Literal(None, ""),
                        ),
                    )

            return exp

        query.transform_expressions(process_all)

        condition = query.get_condition()
        if condition:
            query.set_ast_condition(condition.transform(process_condition))

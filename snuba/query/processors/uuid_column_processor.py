from typing import List
import uuid

from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
    FUNCTION_TO_OPERATOR,
    is_condition,
)
from snuba.query.logical import Query
from snuba.query.matchers import (
    AnyOptionalString,
    Or,
    Param,
    String,
)
from snuba.query.matchers import Column as ColumnMatch
from snuba.query.matchers import FunctionCall as FunctionCallMatch
from snuba.query.matchers import Literal as LiteralMatch
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class UUIDColumnProcessor(QueryProcessor):
    """
    If a condition is being performed on a column that stores UUIDs (as defined in the constructor)
    then change the condition to use a proper UUID instead of a string.
    """

    def formatted_uuid_pattern(self, suffix: str = "") -> FunctionCallMatch:
        return FunctionCallMatch(
            Param("format_alias" + suffix, AnyOptionalString()),
            String("replaceAll"),
            (
                FunctionCallMatch(
                    None,
                    String("toString"),
                    (
                        Param(
                            "formatted_uuid_column" + suffix,
                            ColumnMatch(None, None, self.__uuid_column_match),
                        ),
                    ),
                ),
            ),
            with_optionals=True,
        )

    def uuid_column_pattern(self, suffix: str = "") -> Param:
        return Param(
            "uuid_column" + suffix, ColumnMatch(None, None, self.__uuid_column_match)
        )

    def __init__(self, uuid_columns: List[str]) -> None:
        self.__unique_uuid_columns = set(uuid_columns)
        self.__uuid_column_match = Or(
            [String(u_col) for u_col in self.__unique_uuid_columns]
        )
        self.uuid_in_condition = FunctionCallMatch(
            None,
            Or((String(ConditionFunctions.IN), String(ConditionFunctions.NOT_IN))),
            (
                Or((self.uuid_column_pattern(), self.formatted_uuid_pattern())),
                Param("params", FunctionCallMatch(None, String("tuple"), None)),
            ),
        )
        self.uuid_condition = FunctionCallMatch(
            None,
            Or([String(op) for op in FUNCTION_TO_OPERATOR]),
            (
                Or(
                    (
                        Param("literal_0", LiteralMatch(None, AnyOptionalString())),
                        self.uuid_column_pattern("_0"),
                        self.formatted_uuid_pattern("_0"),
                    )
                ),
                Or(
                    (
                        Param("literal_1", LiteralMatch(None, AnyOptionalString())),
                        self.uuid_column_pattern("_1"),
                        self.formatted_uuid_pattern("_1"),
                    )
                ),
            ),
        )

    def parse_uuid(self, lit: Expression) -> Expression:
        if not isinstance(lit, Literal):
            return lit

        try:
            parsed = uuid.UUID(lit.value)
            return Literal(lit.alias, str(parsed))
        except Exception:
            return lit

    def process_condition(self, exp: Expression) -> Expression:
        result = self.uuid_in_condition.match(exp)
        if result:
            new_params = []
            if result.optional_expression("formatted_uuid_column") is not None:
                alias = result.string("format_alias")
                column = result.expression("formatted_uuid_column")
                new_params.append(Column(alias, column.table_name, column.column_name,))
            else:
                new_params.append(result.expression("uuid_column"))

            params_fn = result.expression("params")
            new_fn_params = []
            for param in params_fn.parameters:
                new_fn_params.append(self.parse_uuid(param))

            new_params.append(
                FunctionCall(
                    params_fn.alias, params_fn.function_name, tuple(new_fn_params)
                )
            )

            return binary_condition(exp.alias, exp.function_name, *new_params)

        result = self.uuid_condition.match(exp)
        if result:
            new_params = []
            for suffix in ["_0", "_1"]:
                if result.optional_expression("literal" + suffix):
                    new_params.append(
                        self.parse_uuid(result.expression("literal" + suffix))
                    )
                elif result.optional_expression("uuid_column" + suffix):
                    new_params.append(result.expression("uuid_column" + suffix))
                elif result.optional_expression("formatted_uuid_column" + suffix):
                    alias = result.optional_string("format_alias" + suffix)
                    column = result.expression("formatted_uuid_column" + suffix)
                    new_params.append(
                        Column(alias, column.table_name, column.column_name,)
                    )

            return binary_condition(exp.alias, exp.function_name, *new_params)

        return exp

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        condition = query.get_condition_from_ast()
        if condition:
            query.set_ast_condition(None)
            for cond in condition:
                if is_condition(cond):
                    query.add_condition_to_ast(self.process_condition(cond))

        prewhere = query.get_prewhere_ast()
        if prewhere:
            query.set_prewhere_ast_condition(None)
            for cond in prewhere:
                if is_condition(cond):
                    query.add_prewhere_condition_to_ast(self.process_condition(cond))

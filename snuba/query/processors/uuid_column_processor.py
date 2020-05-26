from typing import List, Set
import uuid

from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
    FUNCTION_TO_OPERATOR,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
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
            String("replaceAll"),
            (
                FunctionCallMatch(
                    String("toString"),
                    (
                        Param(
                            "formatted_uuid_column" + suffix,
                            ColumnMatch(None, self.__uuid_column_match),
                        ),
                    ),
                ),
            ),
            with_optionals=True,
        )

    def uuid_column_pattern(self, suffix: str = "") -> Param[Column]:
        return Param(
            "uuid_column" + suffix, ColumnMatch(None, self.__uuid_column_match)
        )

    def __init__(self, uuid_columns: Set[str]) -> None:
        self.__unique_uuid_columns = set(uuid_columns)
        self.__uuid_column_match = Or(
            [String(u_col) for u_col in self.__unique_uuid_columns]
        )
        self.uuid_in_condition = FunctionCallMatch(
            Or((String(ConditionFunctions.IN), String(ConditionFunctions.NOT_IN))),
            (
                Or((self.uuid_column_pattern(), self.formatted_uuid_pattern())),
                Param("params", FunctionCallMatch(String("tuple"), None)),
            ),
        )
        self.uuid_condition = FunctionCallMatch(
            Or([String(op) for op in FUNCTION_TO_OPERATOR]),
            (
                Or(
                    (
                        Param("literal_0", LiteralMatch(AnyOptionalString())),
                        self.uuid_column_pattern("_0"),
                        self.formatted_uuid_pattern("_0"),
                    )
                ),
                Or(
                    (
                        Param("literal_1", LiteralMatch(AnyOptionalString())),
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
            parsed = uuid.UUID(str(lit.value))
            return Literal(lit.alias, str(parsed))
        except Exception:
            return lit

    def process_condition(self, exp: Expression) -> Expression:
        if not isinstance(exp, FunctionCall):
            return exp

        result = self.uuid_in_condition.match(exp)
        if result is not None:
            new_params: List[Expression] = []
            if result.contains("formatted_uuid_column"):
                column = result.expression("formatted_uuid_column")
                assert isinstance(column, Column)
                new_params.append(Column(None, column.table_name, column.column_name))
            else:
                column = result.expression("uuid_column")
                assert isinstance(column, Column)
                new_params.append(column)

            params_fn = result.expression("params")
            assert isinstance(params_fn, FunctionCall)
            new_fn_params = []
            for param in params_fn.parameters:
                new_fn_params.append(self.parse_uuid(param))

            new_params.append(
                FunctionCall(
                    params_fn.alias, params_fn.function_name, tuple(new_fn_params)
                )
            )

            return binary_condition(
                exp.alias, exp.function_name, new_params[0], new_params[1]
            )

        result = self.uuid_condition.match(exp)
        if result is not None:
            new_params = []
            for suffix in ["_0", "_1"]:
                if result.contains("literal" + suffix):
                    new_params.append(
                        self.parse_uuid(result.expression("literal" + suffix))
                    )
                elif result.contains("uuid_column" + suffix):
                    new_params.append(result.expression("uuid_column" + suffix))
                elif result.contains("formatted_uuid_column" + suffix):
                    column = result.expression("formatted_uuid_column" + suffix)
                    assert isinstance(column, Column)
                    new_params.append(
                        Column(None, column.table_name, column.column_name)
                    )

            return binary_condition(
                exp.alias, exp.function_name, new_params[0], new_params[1]
            )

        return exp

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        condition = query.get_condition_from_ast()
        if condition:
            query.set_ast_condition(condition.transform(self.process_condition))

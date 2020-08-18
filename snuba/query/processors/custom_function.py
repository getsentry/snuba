from dataclasses import replace
from typing import Sequence

from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column, Expression, FunctionCall
from snuba.query.logical import Query
from snuba.query.parser.expressions import parse_clickhouse_function
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class InvalidCustomFunctionCall(InvalidExpressionException):
    def __str__(self) -> str:
        return f"Invalid custom function call {self.expression}: {self.message}"


class CustomFunction(QueryProcessor):
    """
    Defines a custom snuba function.
    The custom function has a name, a signature in the form of a list of
    parameter names and an expanded expression which is the body of the
    custom function.

    The custom function is invoked in a query as a standard snuba function.

    Example:
    CustomFunction(name="power_two", param_names=[value], body="value * value")

    would transform
    `["power_two", ["f", ["something"]], "alias"]`
    into
    `f(something) * f(something) AS alias`

    Would raise InvalidCustomFunctionCall if a custom function is invoked with
    the wrong number of parameters.
    """

    def __init__(self, name: str, param_names: Sequence[str], body: str) -> None:
        self.__function_name = name
        self.__param_names = param_names
        self.__body = parse_clickhouse_function(body)

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def apply_function(expression: Expression) -> Expression:
            if (
                isinstance(expression, FunctionCall)
                and expression.function_name == self.__function_name
            ):
                if len(expression.parameters) != len(self.__param_names):
                    raise InvalidCustomFunctionCall(
                        expression,
                        (
                            f"Invalid number of parameters for function {self.__function_name}. "
                            f"Required {self.__param_names}"
                        ),
                    )

                resolved_params = {
                    name: expression
                    for (name, expression) in zip(
                        self.__param_names, expression.parameters
                    )
                }

                ret = self.__body.transform(
                    lambda exp: resolved_params[exp.column_name]
                    if isinstance(exp, Column) and exp.column_name in resolved_params
                    else exp
                )
                return replace(ret, alias=expression.alias)
            else:
                return expression

        query.transform_expressions(apply_function)

from dataclasses import replace
from typing import Any, Mapping, Sequence, Tuple

from snuba.clickhouse.columns import ColumnSet
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.parser.expressions import parse_clickhouse_function
from snuba.query.processors import QueryProcessor
from snuba.query.validation import InvalidFunctionCall
from snuba.query.validation.signature import ParamType, SignatureValidator
from snuba.request.request_settings import RequestSettings


class InvalidCustomFunctionCall(InvalidExpressionException):
    def __str__(self) -> str:
        return f"Invalid custom function call {self.expression}: {self.message}"


def replace_in_expression(
    body: Expression, replace_lookup: Mapping[str, Expression]
) -> Expression:
    ret = body.transform(
        lambda exp: replace_lookup[exp.column_name]
        if isinstance(exp, Column) and exp.column_name in replace_lookup
        else exp
    )
    return ret


def simple_function(body: str) -> Expression:
    return parse_clickhouse_function(body)


def partial_function(body: str, constants: Sequence[Tuple[str, Any]]) -> Expression:
    parsed = parse_clickhouse_function(body)
    constants_lookup = {name: Literal(None, value) for (name, value) in constants}
    return replace_in_expression(parsed, constants_lookup)


class CustomFunction(QueryProcessor):
    """
    Defines a custom snuba function.
    The custom function has a name, a signature in the form of a list of
    parameter names and types, an expanded expression which is the body
    of the custom function and the dataset abstract schema for validation.

    The custom function is invoked in a query as a standard snuba function.

    Example:
    CustomFunction(name="power_two", param_names=[value], body="value * value", schema)

    would transform
    `["power_two", [["f", ["something"]]], "alias"]`
    into
    `f(something) * f(something) AS alias`

    Would raise InvalidCustomFunctionCall if a custom function is invoked with
    the wrong number of parameters or if the parameters are of the wrong type
    according to the SignatureValidator.

    The validation has the same limitations of SignatureValidation in that it only deals
    with columns: no literals or complex functions validation.

    We need to pass the dataset abstract schema to the processor constructor
    because the schema is not populated in the query object when dataset processors
    are executed.
    TODO: Assign the abstract dataset schema to the query object right after parsing.
    """

    def __init__(
        self,
        dataset_schema: ColumnSet,
        name: str,
        signature: Sequence[Tuple[str, ParamType]],
        body: Expression,
    ) -> None:
        self.__dataset_schema = dataset_schema
        self.__function_name = name
        self.__param_names = []
        param_types = []
        if len(signature) > 0:
            self.__param_names, param_types = zip(*signature)
        self.__body = body
        self.__validator = SignatureValidator(param_types)

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def apply_function(expression: Expression) -> Expression:
            if (
                isinstance(expression, FunctionCall)
                and expression.function_name == self.__function_name
            ):
                try:
                    self.__validator.validate(
                        expression.parameters, self.__dataset_schema
                    )
                except InvalidFunctionCall as exception:
                    raise InvalidCustomFunctionCall(
                        expression,
                        f"Illegal call to function {expression.function_name}: {str(exception)}",
                    ) from exception

                resolved_params = {
                    name: expression
                    for (name, expression) in zip(
                        self.__param_names, expression.parameters
                    )
                }

                ret = replace_in_expression(self.__body, resolved_params)
                return replace(ret, alias=expression.alias)
            else:
                return expression

        query.transform_expressions(apply_function)

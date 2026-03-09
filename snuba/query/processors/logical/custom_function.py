from dataclasses import replace
from typing import Any, Mapping, Sequence, Tuple

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.clickhouse.columns import UInt
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.parser.expressions import parse_clickhouse_function
from snuba.query.processors.logical import (
    LogicalQueryProcessor,
    ProcessorUnsupportedFromConfig,
)
from snuba.query.query_settings import QuerySettings
from snuba.query.validation import InvalidFunctionCall
from snuba.query.validation.signature import Column as ColType
from snuba.query.validation.signature import Literal as LiteralType
from snuba.query.validation.signature import ParamType, SignatureValidator


class InvalidCustomFunctionCall(InvalidExpressionException):
    def __str__(self) -> str:
        return (
            f"Invalid custom function call {self.extra_data.get('expression', '')}: {self.message}"
        )


def replace_in_expression(body: Expression, replace_lookup: Mapping[str, Expression]) -> Expression:
    ret = body.transform(
        lambda exp: (
            replace_lookup[exp.column_name]
            if isinstance(exp, Column) and exp.column_name in replace_lookup
            else exp
        )
    )
    return ret


def simple_function(body: str) -> Expression:
    return parse_clickhouse_function(body)


def partial_function(body: str, constants: Sequence[Tuple[str, Any]]) -> Expression:
    parsed = parse_clickhouse_function(body)
    constants_lookup = {name: Literal(None, value) for (name, value) in constants}
    return replace_in_expression(parsed, constants_lookup)


class _CustomFunction(LogicalQueryProcessor):
    """
    WARNING
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        This query processor is not referencable from config

        If you want to define a custom function processor accessible from config,
        wrap this class in a class that has __init__ arguments which can be
        expressed in the YAML

        Example: ApdexProcessor, FailureRateProcessor

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

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
        name: str,
        signature: Sequence[Tuple[str, ParamType]],
        body: Expression,
    ) -> None:
        self.__function_name = name
        self.__param_names: Sequence[str] = []
        param_types: Sequence[ParamType] = []
        if len(signature) > 0:
            self.__param_names, param_types = zip(*signature)
        self.__body = body
        self.__validator = SignatureValidator(param_types)

    @classmethod
    def config_key(cls) -> str:
        return "__unsupported__:CustomFunction"

    @classmethod
    def from_kwargs(cls, **kwargs: str) -> LogicalQueryProcessor:
        raise ProcessorUnsupportedFromConfig(
            f"{cls.__name__} is cannot be referenced from configuration because it has a constructor with a python object parameter, either change the constructor to take a basic type (e.g. str, int) or reconsider using this processor"
        )

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def apply_function(expression: Expression) -> Expression:
            if (
                isinstance(expression, FunctionCall)
                and expression.function_name == self.__function_name
            ):
                try:
                    self.__validator.validate(
                        self.__function_name,
                        expression.parameters,
                        query.get_from_clause(),
                    )
                except InvalidFunctionCall as exception:
                    raise InvalidCustomFunctionCall.from_args(
                        expression,
                        f"Illegal call to function {expression.function_name}: {str(exception)}",
                        should_report=False,
                    ) from exception

                resolved_params = {
                    name: expression
                    for (name, expression) in zip(self.__param_names, expression.parameters)
                }

                ret = replace_in_expression(self.__body, resolved_params)
                return replace(ret, alias=expression.alias)
            else:
                return expression

        query.transform_expressions(apply_function)


class ApdexProcessor(LogicalQueryProcessor):
    def __init__(self) -> None:
        self.__processor = _CustomFunction(
            "apdex",
            [("column", ColType({UInt})), ("satisfied", LiteralType({int}))],
            simple_function(
                "divide(plus(countIf(lessOrEquals(column, satisfied)), divide(countIf(and(greater(column, satisfied), lessOrEquals(column, multiply(satisfied, 4)))), 2)), count())"
            ),
        )

    def process_query(self, query: Query, settings: QuerySettings) -> None:
        return self.__processor.process_query(query, settings)


class FailureRateProcessor(LogicalQueryProcessor):
    def __init__(self) -> None:
        self.__processor = _CustomFunction(
            "failure_rate",
            [],
            partial_function(
                # We use and(notEquals...) here instead of in(tuple(...)) because it's possible to get an impossible query that sets transaction_status to NULL.
                # Clickhouse returns an error if an expression such as NULL in (0, 1, 2) appears.
                "divide(countIf(and(notEquals(transaction_status, ok), and(notEquals(transaction_status, cancelled), notEquals(transaction_status, unknown)))), count())",
                [(code, SPAN_STATUS_NAME_TO_CODE[code]) for code in ("ok", "cancelled", "unknown")],
            ),
        )

    def process_query(self, query: Query, settings: QuerySettings) -> None:
        return self.__processor.process_query(query, settings)

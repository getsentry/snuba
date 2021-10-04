from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import (
    Argument,
    Column,
    Expression,
    FunctionCall,
    Lambda,
    Literal,
)
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.query.validation import InvalidFunctionCall
from snuba.query.validation.signature import SignatureValidator
from snuba.request.request_settings import RequestSettings


class HandledFunctionsProcessor(QueryProcessor):
    """
    Adds the isHandled and notHandled snuba functions.

    The implementation of these functions is too complex for clients to provide so
    these wrappers are required.

    - The `isHandled` function searches an array field for null or 1.
    - The `notHandled` function searches an array field for 0, null
      values will be excluded from the result.

    Both functions return 1 or 0 if a row matches.
    """

    def __init__(self, column: str):
        self.__column = column

    def validate_parameters(self, exp: FunctionCall, entity: QueryEntity) -> None:
        validator = SignatureValidator([])
        try:
            validator.validate(exp.function_name, exp.parameters, entity)
        except InvalidFunctionCall as err:
            raise InvalidExpressionException.from_args(
                exp,
                f"Illegal function call to {exp.function_name}: {str(err)}",
                should_report=False,
            ) from err

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall):
                if exp.function_name == "isHandled":
                    self.validate_parameters(exp, query.get_from_clause())
                    return FunctionCall(
                        exp.alias,
                        "arrayExists",
                        (
                            Lambda(
                                None,
                                ("x",),
                                binary_condition(
                                    BooleanFunctions.OR,
                                    FunctionCall(
                                        None, "isNull", (Argument(None, "x"),)
                                    ),
                                    binary_condition(
                                        ConditionFunctions.EQ,
                                        FunctionCall(
                                            None,
                                            "assumeNotNull",
                                            (Argument(None, "x"),),
                                        ),
                                        Literal(None, 1),
                                    ),
                                ),
                            ),
                            Column(None, None, self.__column),
                        ),
                    )
                if exp.function_name == "notHandled":
                    self.validate_parameters(exp, query.get_from_clause())
                    return FunctionCall(
                        exp.alias,
                        "arrayExists",
                        (
                            Lambda(
                                None,
                                ("x",),
                                binary_condition(
                                    BooleanFunctions.AND,
                                    FunctionCall(
                                        None, "isNotNull", (Argument(None, "x"),)
                                    ),
                                    binary_condition(
                                        ConditionFunctions.EQ,
                                        FunctionCall(
                                            None,
                                            "assumeNotNull",
                                            (Argument(None, "x"),),
                                        ),
                                        Literal(None, 0),
                                    ),
                                ),
                            ),
                            Column(None, None, self.__column),
                        ),
                    )
            return exp

        query.transform_expressions(process_functions)

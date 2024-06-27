from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import SimpleDataSource
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
from snuba.query.processors.logical import LogicalQueryProcessor
from snuba.query.query_settings import QuerySettings
from snuba.query.validation import InvalidFunctionCall
from snuba.query.validation.signature import SignatureValidator


class HandledFunctionsProcessor(LogicalQueryProcessor):
    """
    Adds the isHandled and notHandled snuba functions.

    The implementation of these functions is too complex for clients to provide so
    these wrappers are required.

    An event is considered unhandled if at least one of its stacktraces are
    unhandled, regardless if the remaining stacktraces are handled or null. And the
    inverse should hold true for whether an event is considered handled, that all of
    its stacktraces are either handled or null.

    - The `isHandled` function checks the entire array field is null or 1.
    - The `notHandled` function searches an array field for at least one occurrence of
      0

    Both functions return 1 or 0 if a row matches.
    """

    def __init__(self, column: str):
        self.__column = column

    def validate_parameters(
        self, exp: FunctionCall, data_source: SimpleDataSource
    ) -> None:
        validator = SignatureValidator([])
        try:
            validator.validate(exp.function_name, exp.parameters, data_source)
        except InvalidFunctionCall as err:
            raise InvalidExpressionException.from_args(
                exp,
                f"Illegal function call to {exp.function_name}: {str(err)}",
                should_report=False,
            ) from err

    def process_query(self, query: Query, query_settings: QuerySettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall):
                if exp.function_name == "isHandled":
                    self.validate_parameters(exp, query.get_from_clause())
                    return FunctionCall(
                        exp.alias,
                        "arrayAll",
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
                                        ConditionFunctions.NEQ,
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

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.query.conditions import (
    binary_condition,
    combine_and_conditions,
    ConditionFunctions,
)
from snuba.query.dsl import count, divide
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class FailureRateProcessor(QueryProcessor):
    """
    A percentage of transactions with a bad status. "Bad" status is defined as anything other than "ok" and "unknown".
    See here (https://github.com/getsentry/relay/blob/master/py/sentry_relay/consts.py) for the full list of errors.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall) and exp.function_name == "failure_rate":
                assert len(exp.parameters) == 0

                successful_codes = ["ok", "cancelled", "unknown_error"]
                return divide(
                    # We use a FunctionCall directly rather than the countIf wrapper
                    # because of a type hint incompatibility where countIf expects
                    # a FunctionCall but combine_and_conditions returns an Expression
                    FunctionCall(
                        None,
                        "countIf",
                        (
                            combine_and_conditions(
                                [
                                    binary_condition(
                                        None,
                                        ConditionFunctions.NEQ,
                                        Column(None, None, "transaction_status"),
                                        Literal(None, SPAN_STATUS_NAME_TO_CODE[code]),
                                    )
                                    for code in successful_codes
                                ]
                            ),
                        ),
                    ),
                    count(),
                    exp.alias,
                )

            return exp

        query.transform_expressions(process_functions)

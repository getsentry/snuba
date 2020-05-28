from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.dsl import count, countIf, div
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
)
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class ErrorRateProcessor(QueryProcessor):
    """
    A percentage of transactions with a bad status. "Bad" status is defined as anything other than "ok" and "unknown".
    See here (https://github.com/getsentry/relay/blob/master/py/sentry_relay/consts.py) for the full list of errors.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall) and exp.function_name == "error_rate":
                assert len(exp.parameters) == 0

                return div(
                    countIf(
                        binary_condition(
                            None,
                            BooleanFunctions.AND,
                            binary_condition(
                                None,
                                ConditionFunctions.NEQ,
                                Column(None, None, "transaction_status"),
                                Literal(None, SPAN_STATUS_NAME_TO_CODE["ok"]),
                            ),
                            binary_condition(
                                None,
                                ConditionFunctions.NEQ,
                                Column(None, None, "transaction_status"),
                                Literal(
                                    None, SPAN_STATUS_NAME_TO_CODE["unknown_error"]
                                ),
                            ),
                        )
                    ),
                    count(),
                    exp.alias,
                )

            return exp

        query.transform_expressions(process_functions)

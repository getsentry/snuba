from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
)
from snuba.query.dsl import count, countIf, divide, literals_tuple
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
                    countIf(
                        binary_condition(
                            None,
                            ConditionFunctions.NOT_IN,
                            Column(None, None, "transaction_status"),
                            literals_tuple(
                                None,
                                [
                                    Literal(None, SPAN_STATUS_NAME_TO_CODE[code])
                                    for code in successful_codes
                                ],
                            ),
                        ),
                    ),
                    count(),
                    exp.alias,
                )

            return exp

        query.transform_expressions(process_functions)

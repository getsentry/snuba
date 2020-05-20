from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
)
from snuba.query.dsl import count, countIf, div
from snuba.query.expressions import (
    Expression,
    FunctionCall,
    Literal,
    Column,
)
from snuba.query.logical import Query
from snuba.query.processors import QueryProcessor
from snuba.request.request_settings import RequestSettings


class ErrorRateProcessor(QueryProcessor):
    """
    A percentage of transactions with a bad status. "Bad" status is defined as anything other than 0 (success) and 2 (unknown).
    See here (https://github.com/getsentry/relay/blob/master/py/sentry_relay/consts.py) for the full list of errors.

    divide(countIf(and(notEquals(transaction_status, 0), notEquals(transaction_status, 2))), count())
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        def process_functions(exp: Expression) -> Expression:
            if isinstance(exp, FunctionCall) and exp.function_name == "error_rate":
                assert len(exp.parameters) == 0

                return div(
                    countIf(
                        FunctionCall(
                            None,
                            "and",
                            (
                                binary_condition(
                                    None,
                                    ConditionFunctions.NEQ,
                                    Column(None, "transaction_status", None),
                                    Literal(None, 0),
                                ),
                                binary_condition(
                                    None,
                                    ConditionFunctions.NEQ,
                                    Column(None, "transaction_status", None),
                                    Literal(None, 2),
                                ),
                            ),
                        )
                    ),
                    count(),
                )

            return exp

        query.transform_expressions(process_functions)

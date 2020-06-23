from dataclasses import replace

from snuba.clickhouse.processors import QueryProcessor
from snuba.clickhouse.query import Query
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Expression
from snuba.query.expressions import FunctionCall as FunctionCallExpr
from snuba.query.expressions import Literal
from snuba.query.matchers import Column, FunctionCall, Or, String
from snuba.request.request_settings import RequestSettings


class EventsBooleanContextsProcessor(QueryProcessor):
    """
    When Discover started using contexts it turned out that, if we return
    promoted contexts through the contexts[...] syntax we have an inconsistency
    between errors and transactions, and this breaks discover queries
    since the common columns (events/transactions) should behave consistently
    there.
    Specifically contexts[device.simulator] would return 'True' from the
    transactions table and '1' from the events table since the context is
    promoted in the events table (thus stored in a UInt8) but not in the
    transactions table where the context is only in the contexts column
    as a string.

    Boolean context promotion cannot be supported as long as tags/context
    are strings and there is no consistent two ways translation back and
    forth between input and output, so this processor is meant to add a
    patch to the events storage for as long as it exists.
    """

    def process_query(self, query: Query, request_settings: RequestSettings) -> None:
        # We care only of promoted contexts, so we do not need to match
        # the original nested expression.
        matcher = FunctionCall(
            None,
            String("toString"),
            (
                Column(
                    None,
                    None,
                    Or(
                        [
                            String("device_simulator"),
                            String("device_online"),
                            String("device_charging"),
                        ]
                    ),
                ),
            ),
        )

        def replace_exp(exp: Expression) -> Expression:
            if matcher.match(exp) is not None:
                inner = replace(exp, alias=None)
                return FunctionCallExpr(
                    exp.alias,
                    "multiIf",
                    (
                        binary_condition(
                            None, ConditionFunctions.EQ, inner, Literal(None, "")
                        ),
                        Literal(None, ""),
                        binary_condition(
                            None,
                            ConditionFunctions.IN,
                            inner,
                            literals_tuple(
                                None, [Literal(None, "1"), Literal(None, "True")]
                            ),
                        ),
                        Literal(None, "True"),
                        Literal(None, "False"),
                    ),
                )
            return exp

        query.transform_expressions(replace_exp)

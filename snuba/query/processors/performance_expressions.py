from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.clickhouse.columns import ColumnSet, UInt
from snuba.query.validation.signature import Column as ColType, Literal as LiteralType
from snuba.query.processors.custom_function import (
    CustomFunction,
    partial_function,
    simple_function,
)


def apdex_processor(columns: ColumnSet) -> CustomFunction:
    return CustomFunction(
        columns,
        "apdex",
        [("column", ColType({UInt})), ("satisfied", LiteralType({int}))],
        simple_function(
            "divide(plus(countIf(lessOrEquals(column, satisfied)), divide(countIf(and(greater(column, satisfied), lessOrEquals(column, multiply(satisfied, 4)))), 2)), count())"
        ),
    )


def failure_rate_processor(columns: ColumnSet) -> CustomFunction:
    return CustomFunction(
        columns,
        "failure_rate",
        [],
        partial_function(
            "divide(countIf(notIn(transaction_status, tuple(ok, cancelled, unknown))), count())",
            [
                (code, SPAN_STATUS_NAME_TO_CODE[code])
                for code in ("ok", "cancelled", "unknown")
            ],
        ),
    )

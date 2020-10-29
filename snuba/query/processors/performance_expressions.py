from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.clickhouse.columns import ColumnSet, SchemaModifiers, UInt
from snuba.query.validation.signature import Column as ColType, Literal as LiteralType
from snuba.query.processors.custom_function import (
    CustomFunction,
    partial_function,
    simple_function,
)


def apdex_processor(columns: ColumnSet[SchemaModifiers]) -> CustomFunction:
    return CustomFunction(
        columns,
        "apdex",
        [("column", ColType({UInt})), ("satisfied", LiteralType({int}))],
        simple_function(
            "divide(plus(countIf(lessOrEquals(column, satisfied)), divide(countIf(and(greater(column, satisfied), lessOrEquals(column, multiply(satisfied, 4)))), 2)), count())"
        ),
    )


def failure_rate_processor(columns: ColumnSet[SchemaModifiers]) -> CustomFunction:
    return CustomFunction(
        columns,
        "failure_rate",
        [],
        partial_function(
            # We use and(notEquals...) here instead of in(tuple(...)) because it's possible to get an impossible query that sets transaction_status to NULL.
            # Clickhouse returns an error if an expression such as NULL in (0, 1, 2) appears.
            "divide(countIf(and(notEquals(transaction_status, ok), and(notEquals(transaction_status, cancelled), notEquals(transaction_status, unknown)))), count())",
            [
                (code, SPAN_STATUS_NAME_TO_CODE[code])
                for code in ("ok", "cancelled", "unknown")
            ],
        ),
    )

from sentry_relay.consts import SPAN_STATUS_NAME_TO_CODE

from snuba.clickhouse.columns import ColumnSet, UInt
from snuba.query.validation.signature import Column as ColType, Literal as LiteralType
from snuba.query.processors.custom_function import CustomFunction


def apdex_processor(columns: ColumnSet) -> CustomFunction:
    return CustomFunction(
        columns,
        "apdex",
        [("column", ColType({UInt})), ("satisfied", LiteralType({int}))],
        [],
        "divide(plus(countIf(lessOrEquals(column, satisfied)), divide(countIf(and(greater(column, satisfied), lessOrEquals(column, multiply(satisfied, 4)))), 2)), count())",
    )


def failure_rate_processor(columns: ColumnSet) -> CustomFunction:
    return CustomFunction(
        columns,
        "failure_rate",
        [],
        [
            (code, SPAN_STATUS_NAME_TO_CODE[code])
            for code in ("ok", "cancelled", "unknown")
        ],
        "divide(countIf(notIn(transaction_status, tuple(ok, cancelled, unknown))), count())",
    )

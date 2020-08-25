from snuba.clickhouse.columns import ColumnSet, UInt
from snuba.query.validation.signature import Column as ColType, Literal as LiteralType
from snuba.query.processors.custom_function import CustomFunction


def apdex_processor(columns: ColumnSet):
    return CustomFunction(
        columns,
        "apdex",
        [("column", ColType({UInt})), ("satisfied", LiteralType({int}))],
        "divide(plus(countIf(lessOrEquals(column, satisfied)), divide(countIf(and(greater(column, satisfied), lessOrEquals(column, multiply(satisfied, 4)))), 2)), count())",
    )

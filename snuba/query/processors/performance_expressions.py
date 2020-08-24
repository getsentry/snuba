from snuba.clickhouse.columns import ColumnSet, String, UInt
from snuba.query.validation.signature import Column as ColType
from snuba.query.processors.custom_function import CustomFunction


apdex_processor = CustomFunction(
    ColumnSet([("column", String()), ("satisfied", UInt(32))]),
    "apdex",
    [("column", ColType({String})), ("satisfied", ColType({UInt}))],
    "divide(plus(countIf(lessOrEquals(column, satisfied)), divide(countIf(and(greater(column, satisfied), lessOrEquals(column, multiply(satisfied, 4)))), 2)), count())",
)

from dataclasses import dataclass

from snuba import environment
from snuba.clickhouse.translators.snuba.mappers import (
    ColumnToCurriedFunction,
    ColumnToFunction,
)
from snuba.clickhouse.translators.snuba.mapping import TranslationMappers
from snuba.processor import MAX_UINT32, NIL_UUID
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
    in_condition,
)
from snuba.query.expressions import Column, Expression, FunctionCall, Literal
from snuba.utils.metrics.wrapper import MetricsWrapper

metrics = MetricsWrapper(environment.metrics, "api.sessions")


def function_call(col_name: str, function_name: str) -> FunctionCall:
    return FunctionCall(None, function_name, (Column(None, None, col_name),))


@dataclass(frozen=True)
class FunctionColumn(ColumnToFunction):
    def __init__(self, col_name: str, function_name: str):
        super().__init__(
            None,
            col_name,
            function_name,
            (Column(None, None, col_name),),
        )


@dataclass(frozen=True)
class PlusFunctionColumns(ColumnToFunction):
    def __init__(
        self, col_name: str, op1_col: str, op1_func: str, op2_col: str, op2_func: str
    ):
        return super().__init__(
            None,
            col_name,
            "plus",
            (
                function_call(op1_col, op1_func),
                function_call(op2_col, op2_func),
            ),
        )


class DurationQuantilesHourlyMapper(ColumnToCurriedFunction):
    def __init__(self) -> None:
        return super().__init__(
            None,
            "duration_quantiles",
            FunctionCall(None, "quantilesIfMerge", quantiles),
            (Column(None, None, "duration_quantiles"),),
        )


# We have the following columns that we want to query:
# * duration_quantiles
# * duration_avg
# * sessions
# * sessions_crashed
# * sessions_abnormal
# * sessions_errored
# * users
# * users_crashed
# * users_abnormal
# * users_errored

quantiles = tuple(Literal(None, quant) for quant in [0.5, 0.75, 0.9, 0.95, 0.99, 1])


quantity = Column(None, None, "quantity")
seq = Column(None, None, "seq")
status = Column(None, None, "status")
session_id = Column(None, None, "session_id")
distinct_id = Column(None, None, "distinct_id")
duration = Column(None, None, "duration")
lit_nil = Literal(None, NIL_UUID)


def eq(col: Column, lit: Literal) -> FunctionCall:
    return binary_condition(ConditionFunctions.EQ, col, lit)


def neq(col: Column, lit: Literal) -> FunctionCall:
    return binary_condition(ConditionFunctions.NEQ, col, lit)


def _and(ex1: Expression, ex2: Expression) -> FunctionCall:
    return binary_condition(BooleanFunctions.AND, ex1, ex2)


# `errors > 0`
has_errors = binary_condition(
    ConditionFunctions.GT, Column(None, None, "errors"), Literal(None, 0)
)
# `distinct_id != NIL`
did_not_nil = neq(distinct_id, lit_nil)
# `duration != MAX AND status == 1`
duration_condition = _and(
    neq(duration, Literal(None, MAX_UINT32)), eq(status, Literal(None, 1))
)
# `status IN (2,3,4)`
terminal_status = in_condition(status, [Literal(None, status) for status in [2, 3, 4]])

# These here are basically the same statements as the matview query
sessions_raw_translators = TranslationMappers(
    columns=[
        ColumnToCurriedFunction(
            None,
            "duration_quantiles",
            FunctionCall(None, "quantilesIf", quantiles),
            (duration, duration_condition),
        ),
        ColumnToFunction(None, "duration_avg", "avgIf", (duration, duration_condition)),
        ColumnToFunction(
            None,
            "sessions",
            "sumIf",
            (quantity, eq(seq, Literal(None, 0))),
        ),
        ColumnToFunction(
            None, "sessions_crashed", "sumIf", (quantity, eq(status, Literal(None, 2)))
        ),
        ColumnToFunction(
            None, "sessions_abnormal", "sumIf", (quantity, eq(status, Literal(None, 3)))
        ),
        ColumnToFunction(
            None,
            "sessions_errored",
            "plus",
            (
                FunctionCall(
                    None,
                    "uniqIf",
                    (session_id, _and(has_errors, neq(session_id, lit_nil))),
                ),
                FunctionCall(
                    None,
                    "sumIf",
                    (quantity, _and(terminal_status, eq(session_id, lit_nil))),
                ),
            ),
        ),
        ColumnToFunction(None, "users", "uniqIf", (distinct_id, did_not_nil)),
        ColumnToFunction(
            None,
            "users_crashed",
            "uniqIf",
            (distinct_id, _and(eq(status, Literal(None, 2)), did_not_nil)),
        ),
        ColumnToFunction(
            None,
            "users_abnormal",
            "uniqIf",
            (distinct_id, _and(eq(status, Literal(None, 3)), did_not_nil)),
        ),
        ColumnToFunction(
            None,
            "users_errored",
            "uniqIf",
            (distinct_id, _and(has_errors, did_not_nil)),
        ),
    ]
)

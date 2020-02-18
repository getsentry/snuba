from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.dsl import count, countIf, div, multiply, plus
from snuba.query.expressions import (
    Expression,
    Literal,
    Column,
)


def apdex(column: Column, satisfied: Literal) -> Expression:
    tolerated = multiply(satisfied, Literal(None, 4))

    return div(
        plus(
            countIf(binary_condition(None, ConditionFunctions.LTE, column, satisfied)),
            div(
                countIf(
                    binary_condition(
                        None,
                        BooleanFunctions.AND,
                        binary_condition(
                            None, ConditionFunctions.GT, column, satisfied,
                        ),
                        binary_condition(
                            None, ConditionFunctions.LTE, column, tolerated,
                        ),
                    ),
                ),
                Literal(None, 2),
            ),
        ),
        count(),
    )

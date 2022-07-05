from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import divide, multiply, plus
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.performance_expressions import apdex_processor
from snuba.query.query_settings import HTTPQuerySettings


def test_apdex_format_expressions() -> None:
    unprocessed = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression(
                "perf",
                FunctionCall(
                    "perf", "apdex", (Column(None, None, "column1"), Literal(None, 300))
                ),
            ),
        ],
    )
    expected = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression(
                "perf",
                divide(
                    plus(
                        FunctionCall(
                            None,
                            "countIf",
                            (
                                binary_condition(
                                    ConditionFunctions.LTE,
                                    Column(None, None, "column1"),
                                    Literal(None, 300),
                                ),
                            ),
                        ),
                        divide(
                            FunctionCall(
                                None,
                                "countIf",
                                (
                                    binary_condition(
                                        BooleanFunctions.AND,
                                        binary_condition(
                                            ConditionFunctions.GT,
                                            Column(None, None, "column1"),
                                            Literal(None, 300),
                                        ),
                                        binary_condition(
                                            ConditionFunctions.LTE,
                                            Column(None, None, "column1"),
                                            multiply(
                                                Literal(None, 300), Literal(None, 4)
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                            Literal(None, 2),
                        ),
                    ),
                    FunctionCall(
                        None,
                        "count",
                        (),
                    ),
                    "perf",
                ),
            ),
        ],
    )

    apdex_processor().process_query(unprocessed, HTTPQuerySettings())
    assert expected.get_selected_columns() == unprocessed.get_selected_columns()

    ret = unprocessed.get_selected_columns()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(divide(plus(countIf(lessOrEquals(column1, 300)), "
        "divide(countIf(greater(column1, 300) AND "
        "lessOrEquals(column1, multiply(300, 4))), 2)), count()) AS perf)"
    )

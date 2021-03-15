from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    ConditionFunctions,
    binary_condition,
    combine_and_conditions,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.dsl import count, divide
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.performance_expressions import failure_rate_processor
from snuba.request.request_settings import HTTPRequestSettings


def test_failure_rate_format_expressions() -> None:
    unprocessed = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression("perf", FunctionCall("perf", "failure_rate", ())),
        ],
    )
    expected = Query(
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression(
                "perf",
                divide(
                    FunctionCall(
                        None,
                        "countIf",
                        (
                            combine_and_conditions(
                                [
                                    binary_condition(
                                        ConditionFunctions.NEQ,
                                        Column(None, None, "transaction_status"),
                                        Literal(None, code),
                                    )
                                    for code in [0, 1, 2]
                                ]
                            ),
                        ),
                    ),
                    count(),
                    "perf",
                ),
            ),
        ],
    )

    failure_rate_processor(ColumnSet([])).process_query(
        unprocessed, HTTPRequestSettings()
    )
    assert expected.get_selected_columns() == unprocessed.get_selected_columns()

    ret = unprocessed.get_selected_columns()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(divide(countIf(notEquals(transaction_status, 0) AND notEquals(transaction_status, 1) AND notEquals(transaction_status, 2)), count()) AS perf)"
    )

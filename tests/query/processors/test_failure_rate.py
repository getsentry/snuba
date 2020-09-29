from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
)
from snuba.query.dsl import count, divide
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.processors.performance_expressions import failure_rate_processor
from snuba.request.request_settings import HTTPRequestSettings


def test_failure_rate_format_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression("perf", FunctionCall("perf", "failure_rate", ())),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression(
                "perf",
                divide(
                    FunctionCall(
                        None,
                        "countIf",
                        (
                            binary_condition(
                                None,
                                ConditionFunctions.NOT_IN,
                                Column(None, None, "transaction_status"),
                                FunctionCall(
                                    None,
                                    "tuple",
                                    (
                                        Literal(alias=None, value=0),
                                        Literal(alias=None, value=1),
                                        Literal(alias=None, value=2),
                                    ),
                                ),
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
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(divide(countIf(notIn(transaction_status, tuple(0, 1, 2))), count()) AS perf)"
    )

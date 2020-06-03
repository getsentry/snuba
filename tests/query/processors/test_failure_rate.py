from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    binary_condition,
    ConditionFunctions,
)
from snuba.query.dsl import count, countIf, div
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.failure_rate_processor import FailureRateProcessor
from snuba.request.request_settings import HTTPRequestSettings


def test_failure_rate_format_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column(None, None, "column2"),
            FunctionCall("perf", "failure_rate", ()),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column(None, None, "column2"),
            div(
                countIf(
                    FunctionCall(
                        None,
                        "and",
                        (
                            binary_condition(
                                None,
                                ConditionFunctions.NEQ,
                                Column(None, None, "transaction_status"),
                                Literal(None, 0),
                            ),
                            binary_condition(
                                None,
                                ConditionFunctions.NEQ,
                                Column(None, None, "transaction_status"),
                                Literal(None, 2),
                            ),
                        ),
                    )
                ),
                count(),
                "perf",
            ),
        ],
    )

    FailureRateProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(div(countIf(and(notEquals(transaction_status, 0), notEquals(transaction_status, 2))), count()) AS perf)"
    )

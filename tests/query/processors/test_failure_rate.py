from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.dsl import count, countIf, divide
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.processors.failure_rate_processor import FailureRateProcessor
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
                    countIf(
                        FunctionCall(
                            None,
                            "notIn",
                            (
                                FunctionCall(
                                    None,
                                    "coalesce",
                                    (
                                        Column(None, None, "transaction_status"),
                                        FunctionCall(
                                            None, "toUInt8OrNull", (Literal(None, ""),),
                                        ),
                                    ),
                                ),
                                FunctionCall(
                                    None,
                                    "tuple",
                                    (
                                        Literal(None, 0),
                                        Literal(None, 1),
                                        Literal(None, 2),
                                    ),
                                ),
                            ),
                        )
                    ),
                    count(),
                    "perf",
                ),
            ),
        ],
    )

    FailureRateProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(divide(countIf(notIn(coalesce(transaction_status, toUInt8OrNull('')), tuple(0, 1, 2))), count()) AS perf)"
    )

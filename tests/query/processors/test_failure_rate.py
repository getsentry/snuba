from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.dsl import count, countIf, divide
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
            divide(
                countIf(
                    FunctionCall(
                        None,
                        "notIn",
                        (
                            Column(None, None, "transaction_status"),
                            FunctionCall(
                                None,
                                "tuple",
                                (Literal(None, 0), Literal(None, 1), Literal(None, 2)),
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
        "(divide(countIf(notIn(transaction_status, tuple(0, 1, 2))), count()) AS perf)"
    )

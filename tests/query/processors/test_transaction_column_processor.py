from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.datasets.storages.transaction_column_processor import (
    TransactionColumnProcessor,
)
from snuba.query import SelectedExpression
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.clickhouse.query import Query
from snuba.request.request_settings import HTTPRequestSettings


def test_transaction_column_format_expressions() -> None:
    unprocessed = Query(
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression(
                "the_event_id", Column("the_event_id", None, "event_id")
            ),
        ],
    )
    expected = Query(
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "transaction.duration", Column("transaction.duration", None, "duration")
            ),
            SelectedExpression(
                "the_event_id",
                FunctionCall(
                    "the_event_id",
                    "replaceAll",
                    (
                        FunctionCall(
                            None, "toString", (Column(None, None, "event_id"),),
                        ),
                        Literal(None, "-"),
                        Literal(None, ""),
                    ),
                ),
            ),
        ],
    )

    TransactionColumnProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    formatted = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert formatted == "(replaceAll(toString(event_id), '-', '') AS the_event_id)"

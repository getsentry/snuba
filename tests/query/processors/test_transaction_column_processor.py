from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.transaction_column_processor import (
    TransactionColumnProcessor,
)
from snuba.query.logical import Query
from snuba.request.request_settings import HTTPRequestSettings


def test_transaction_column_format_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("transaction.duration", "duration", None),
            Column("the_event_id", "event_id", None),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("transaction.duration", "duration", None),
            FunctionCall(
                "the_event_id",
                "replaceAll",
                (
                    FunctionCall(None, "toString", (Column(None, "event_id", None),),),
                    Literal(None, "-"),
                    Literal(None, ""),
                ),
            ),
        ],
    )

    TransactionColumnProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    formatted = unprocessed.get_selected_columns_from_ast()[1].accept(
        ClickhouseExpressionFormatter()
    )
    assert formatted == "(replaceAll(toString(event_id), '-', '') AS the_event_id)"

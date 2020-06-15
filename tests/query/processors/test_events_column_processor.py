from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.datasets.storages.events_column_processor import EventsColumnProcessor
from snuba.request.request_settings import HTTPRequestSettings


def test_events_column_format_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("dr_claw", None, "culprit"),
            Column("the_group_id", None, "group_id"),
            Column("the_message", None, "message"),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("dr_claw", None, "culprit"),
            FunctionCall(
                "the_group_id",
                "nullIf",
                (Column(None, None, "group_id"), Literal(None, 0),),
            ),
            FunctionCall(
                "the_message",
                "coalesce",
                (Column(None, None, "search_message"), Column(None, None, "message")),
            ),
        ],
    )

    EventsColumnProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    expected = (
        "(nullIf(group_id, 0) AS the_group_id)",
        "(coalesce(search_message, message) AS the_message)",
    )

    for idx, column in enumerate(unprocessed.get_selected_columns_from_ast()[1:]):
        formatted = column.accept(ClickhouseExpressionFormatter())
        assert expected[idx] == formatted

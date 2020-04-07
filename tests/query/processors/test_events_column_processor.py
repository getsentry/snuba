from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.dsl import literals_tuple
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.events_column_processor import EventsColumnProcessor
from snuba.query.query import Query
from snuba.request.request_settings import HTTPRequestSettings


def test_events_column_format_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("dr_claw", "culprit", None),
            Column("the_group_id", "group_id", None),
            Column("the_message", "message", None),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column("dr_claw", "culprit", None),
            FunctionCall(
                "the_group_id",
                "nullIf",
                (Column(None, "group_id", None), Literal(None, 0),),
            ),
            FunctionCall(
                "the_message",
                "coalesce",
                (Column(None, "message", None), Column(None, "search_message", None),),
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
        "(coalesce(message, search_message) AS the_message)",
    )

    for idx, column in enumerate(unprocessed.get_selected_columns_from_ast()[1:]):
        formatted = column.accept(ClickhouseExpressionFormatter())
        assert expected[idx] == formatted

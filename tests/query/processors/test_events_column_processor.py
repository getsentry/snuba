from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.clickhouse.query import Query
from snuba.datasets.storages.storage_key import StorageKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.physical.group_id_column_processor import (
    GroupIdColumnProcessor,
)
from snuba.query.query_settings import HTTPQuerySettings


def test_events_column_format_expressions() -> None:
    unprocessed = Query(
        Table("events", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[
            SelectedExpression("dr_claw", Column("dr_claw", None, "culprit")),
            SelectedExpression("the_group_id", Column("the_group_id", None, "group_id")),
            SelectedExpression("the_message", Column("the_message", None, "message")),
        ],
    )
    expected_query = Query(
        Table("events", ColumnSet([]), storage_key=StorageKey("dontmatter")),
        selected_columns=[
            SelectedExpression("dr_claw", Column("dr_claw", None, "culprit")),
            SelectedExpression(
                "the_group_id",
                FunctionCall(
                    "the_group_id",
                    "nullIf",
                    (
                        Column(None, None, "group_id"),
                        Literal(None, 0),
                    ),
                ),
            ),
            SelectedExpression(
                "the_message",
                Column("the_message", None, "message"),
            ),
        ],
    )

    GroupIdColumnProcessor().process_query(unprocessed, HTTPQuerySettings())
    assert expected_query.get_selected_columns() == unprocessed.get_selected_columns()

    expected = (
        "(nullIf(group_id, 0) AS the_group_id)",
        "(message AS the_message)",
    )

    for idx, column in enumerate(unprocessed.get_selected_columns()[1:]):
        formatted = column.expression.accept(ClickhouseExpressionFormatter())
        assert expected[idx] == formatted
